package grpc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const (
	bufSize = 1024 * 1024

	// bufTarget 是测试内 GRPCChecker.addr 的取值。
	// 说明：grpc.NewClient 对无 scheme 目标走默认 dns resolver，"bufnet" 无法解析
	// （lookup 失败 → ReportError → RPC 以 Unavailable 失败，永远到不了自定义 dialer）；
	// passthrough scheme 将端点直通交给 WithContextDialer，是 grpc 官方测试搭配
	// bufconn/自定义 dialer 的接线方式（v1.75.1 test/end2end_test.go:4732）。
	bufTarget = "passthrough:///bufnet"
)

// newBufServer 启动 bufconn 上的 grpc.Server；withHealth=true 时注册官方
// health.NewServer() 并返回其句柄（withHealth=false 时句柄为 nil）。
// 服务器停止由 t.Cleanup 托管；Serve 退出错误（ErrServerStopped 除外）经 assert 上报。
func newBufServer(t *testing.T, withHealth bool) (*bufconn.Listener, *health.Server) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	srv := grpclib.NewServer()
	var hs *health.Server
	if withHealth {
		hs = health.NewServer()
		healthpb.RegisterHealthServer(srv, hs)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		err := <-serveErr
		if !errors.Is(err, grpclib.ErrServerStopped) {
			assert.NoError(t, err, "bufconn grpc.Server.Serve exited abnormally")
		}
	})
	return lis, hs
}

// bufDialOpts 返回将 gRPC 连接路由到 bufconn listener 的 DialOption
// （GRPCChecker.dialOpts 测试注入 seam）。
func bufDialOpts(lis *bufconn.Listener) []grpclib.DialOption {
	return []grpclib.DialOption{
		grpclib.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	}
}

// newBufChecker 直构 GRPCChecker（不经 factory），探测经 bufconn 走真实 RPC。
func newBufChecker(lis *bufconn.Listener, service string) *GRPCChecker {
	return &GRPCChecker{
		addr:     bufTarget,
		timeout:  2 * time.Second,
		service:  service,
		dialOpts: bufDialOpts(lis),
	}
}

// TestGRPCChecker_FourStates 覆盖 C10 四态语义（docs/09 §5.3 + R-c 裁定）：
// SERVING / NOT_SERVING / NOT_FOUND（service 未设置）/ UNIMPLEMENTED（未注册 health 服务）。
func TestGRPCChecker_FourStates(t *testing.T) {
	tests := []struct {
		name            string
		withHealth      bool
		setup           func(hs *health.Server)
		service         string
		wantHealthy     bool
		wantStatusCode  int
		wantErr         bool   // 断言 result.Err != nil
		wantErrContains string // 非空时额外断言 Err 消息包含该子串
	}{
		{
			name:       "(1) SERVING -> Healthy=true, StatusCode=int(codes.OK)",
			withHealth: true,
			setup: func(hs *health.Server) {
				hs.SetServingStatus("svc-up", healthpb.HealthCheckResponse_SERVING)
			},
			service:        "svc-up",
			wantHealthy:    true,
			wantStatusCode: int(codes.OK),
		},
		{
			name:       "(2) NOT_SERVING -> Healthy=false, StatusCode=int(codes.OK), Err contains NOT_SERVING",
			withHealth: true,
			setup: func(hs *health.Server) {
				hs.SetServingStatus("svc-down", healthpb.HealthCheckResponse_NOT_SERVING)
			},
			service:         "svc-down",
			wantHealthy:     false,
			wantStatusCode:  int(codes.OK),
			wantErr:         true,
			wantErrContains: "NOT_SERVING",
		},
		{
			name:       "(3) service not set -> Healthy=false, StatusCode=int(codes.NotFound)",
			withHealth: true,
			setup: func(hs *health.Server) {
				hs.SetServingStatus("svc-up", healthpb.HealthCheckResponse_SERVING)
			},
			service:        "svc-unknown",
			wantHealthy:    false,
			wantStatusCode: int(codes.NotFound),
			wantErr:        true,
		},
		{
			name:           "(4) health service not registered -> Healthy=false, StatusCode=int(codes.Unimplemented)",
			withHealth:     false,
			service:        "",
			wantHealthy:    false,
			wantStatusCode: int(codes.Unimplemented),
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lis, hs := newBufServer(t, tt.withHealth)
			if tt.setup != nil {
				require.NotNil(t, hs)
				tt.setup(hs)
			}
			c := newBufChecker(lis, tt.service)

			result, err := c.Check(context.Background())
			require.NoError(t, err, "M-5: builtin Checker failure paths return facts (result, nil)")
			require.NotNil(t, result)

			assert.Equal(t, tt.wantHealthy, result.Healthy)
			assert.Equal(t, tt.wantStatusCode, result.StatusCode)
			switch {
			case tt.wantErrContains != "":
				assert.ErrorContains(t, result.Err, tt.wantErrContains)
			case tt.wantErr:
				assert.Error(t, result.Err)
			default:
				assert.NoError(t, result.Err)
			}
			assert.Greater(t, result.Latency, time.Duration(0))
			assert.True(t, result.Timestamp.IsZero(), "M-6: builtin Checker leaves Timestamp zero")
		})
	}
}

// TestGRPCChecker_ServiceName 验证 Extra["service"] 经注册 factory 路由进
// Health/Check RPC：服务器仅对 "svc-up" 置 SERVING、对 "" 显式置 NOT_SERVING，
// factory 构造（service="svc-up"）必须命中健康；对照组 service="" 必须不健康。
// 注：health.NewServer() 预置 ""→SERVING（v1.75.1 health/server.go NewServer），
// 故对照组需显式 SetServingStatus("", NOT_SERVING) 才具判别力。
func TestGRPCChecker_ServiceName(t *testing.T) {
	lis, hs := newBufServer(t, true)
	require.NotNil(t, hs)
	hs.SetServingStatus("svc-up", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	// 经注册路径构造（同时验证 blank import init() 注册对 NewChecker 可用）。
	c, err := checker.NewChecker("grpc", checker.TargetConfig{
		Addr:    bufTarget,
		Timeout: 2 * time.Second,
		Extra:   map[string]string{"service": "svc-up"},
	})
	require.NoError(t, err)
	require.NotNil(t, c)
	gc, ok := c.(*GRPCChecker)
	require.True(t, ok, `"grpc" factory should return *GRPCChecker`)
	assert.Equal(t, "svc-up", gc.service)
	gc.dialOpts = bufDialOpts(lis)

	result, err := gc.Check(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Healthy, "should hit svc-up (SERVING)")
	assert.Equal(t, int(codes.OK), result.StatusCode)

	// 对照组：整体检查 service=""（同服务器 NOT_SERVING）——若 factory 丢失
	// service 名，上方主检查也会得到 NOT_SERVING，判别力由此确立。
	whole := newBufChecker(lis, "")
	wholeResult, err := whole.Check(context.Background())
	require.NoError(t, err)
	require.NotNil(t, wholeResult)
	assert.False(t, wholeResult.Healthy)
	assert.ErrorContains(t, wholeResult.Err, "NOT_SERVING")
}

// TestGRPCChecker_CancelledContext 验证已取消 ctx 下 Check 不阻塞、
// 事实化返回不健康结果，且 gRPC code 为 Canceled。
func TestGRPCChecker_CancelledContext(t *testing.T) {
	lis, hs := newBufServer(t, true)
	require.NotNil(t, hs)
	c := newBufChecker(lis, "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	var result *checker.CheckResult
	var checkErr error
	go func() {
		defer close(done)
		result, checkErr = c.Check(ctx)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Check blocked without returning under cancelled ctx (select timeout prevents deadlock)")
	}

	require.NoError(t, checkErr, "M-5: factual return (result, nil)")
	require.NotNil(t, result)
	assert.False(t, result.Healthy)
	require.Error(t, result.Err)
	st, ok := status.FromError(result.Err)
	require.True(t, ok, "Err should be a gRPC status error")
	assert.Equal(t, codes.Canceled, st.Code())
	assert.True(t, result.Timestamp.IsZero(), "M-6: builtin Checker leaves Timestamp zero")
}

// TestGRPCChecker_ConnRefused 验证连接失败（listener 已关闭）路径：
// 事实化返回不健康 + Err（codes.Unavailable），不阻塞、不 panic。
func TestGRPCChecker_ConnRefused(t *testing.T) {
	lis := bufconn.Listen(bufSize)
	require.NoError(t, lis.Close())
	c := newBufChecker(lis, "")

	result, err := c.Check(context.Background())
	require.NoError(t, err, "M-5: factual return (result, nil)")
	require.NotNil(t, result)
	assert.False(t, result.Healthy)
	require.Error(t, result.Err)
	st, ok := status.FromError(result.Err)
	require.True(t, ok, "Err should be a gRPC status error")
	assert.Equal(t, codes.Unavailable, st.Code())
	assert.True(t, result.Timestamp.IsZero(), "M-6: builtin Checker leaves Timestamp zero")
}

// TestGRPCChecker_FactoryErrors 验证空地址配置在构造期即失败（不进入探测）。
func TestGRPCChecker_FactoryErrors(t *testing.T) {
	c, err := checker.NewChecker("grpc", checker.TargetConfig{Addr: ""})
	require.Error(t, err)
	assert.Nil(t, c)
	assert.Contains(t, err.Error(), "checker: empty target address") // 消息三子包统一（原 "checker/grpc: " 前缀变体已弃用，docs/06 §5）
	assert.ErrorIs(t, err, checker.ErrEmptyTargetAddress)            // 三个子包共用的 sentinel（docs/06 §5）
}

// TestGRPCChecker_FactoryExtras 验证 factory 对 Extra 的解析：
// service 直读字段（默认 ""）；tls 按合法值域解析（缺省 false = insecure，
// 非法值拒绝路径见 TestGRPCChecker_FactoryTLSValidation）。
func TestGRPCChecker_FactoryExtras(t *testing.T) {
	c, err := newGRPCChecker(checker.TargetConfig{
		Addr:    "example:9000",
		Timeout: time.Second,
		Extra:   map[string]string{"service": "s", "tls": "true"},
	})
	require.NoError(t, err)
	gc, ok := c.(*GRPCChecker)
	require.True(t, ok)
	assert.Equal(t, "s", gc.service)
	assert.True(t, gc.useTLS)
	assert.Equal(t, time.Second, gc.timeout)

	// 无 Extra：tls 缺省 false（insecure），service 缺省 ""（整体检查）。
	c2, err := newGRPCChecker(checker.TargetConfig{Addr: "example:9000"})
	require.NoError(t, err)
	gc2, ok := c2.(*GRPCChecker)
	require.True(t, ok)
	assert.False(t, gc2.useTLS)
	assert.Equal(t, "", gc2.service)
}

// TestGRPCChecker_FactoryTLSValidation 钉住 r4 审计 #P3-3 修复（表驱动）：
// Extra["tls"] 仅接受精确字面量 "true"（启用 TLS）与 "false"（insecure）；
// 键缺失默认 insecure（现状不变）；任何其他值——含 "True"/"1"/"yes" 等非精确
// 字面量与显式空串——构造期直返 ErrInvalidTLSValue（fail-fast，防 TLS 配置
// 误用静默降级为 insecure 明文探测；对齐 checker/http expected_status 的
// 构造期校验风格，docs/05 §5：不静默钳制或回退默认值）。
func TestGRPCChecker_FactoryTLSValidation(t *testing.T) {
	tests := []struct {
		name    string
		value   string // Extra["tls"] 的显式取值；ok=false 表示键缺失
		ok      bool
		wantErr bool
		wantTLS bool
	}{
		{name: "exact_true_enables_tls", value: "true", ok: true, wantTLS: true},
		{name: "exact_false_disables_tls", value: "false", ok: true},
		{name: "missing_key_defaults_insecure", ok: false},
		{name: "capitalized_True_rejected", value: "True", ok: true, wantErr: true},
		{name: "numeric_1_rejected", value: "1", ok: true, wantErr: true},
		{name: "affirmative_yes_rejected", value: "yes", ok: true, wantErr: true},
		{name: "explicit_empty_string_rejected", value: "", ok: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := checker.TargetConfig{Addr: "example:9000"}
			if tt.ok {
				cfg.Extra = map[string]string{"tls": tt.value}
			}
			c, err := checker.NewChecker("grpc", cfg)
			if tt.wantErr {
				require.Error(t, err, "non-exact tls literal must be rejected fail-fast (no silent insecure downgrade)")
				assert.Nil(t, c)
				assert.ErrorIs(t, err, ErrInvalidTLSValue, "the sentinel identifies the config error class")
				if tt.value != "" {
					assert.NotContains(t, err.Error(), tt.value, "the configured value must not be echoed into the error message (docs/07 sec. 5)")
				}
				assert.Equal(t, "checker: invalid tls value", err.Error(),
					"sentinel returned directly without wrapping (message is self-contained, docs/06 sec. 5)")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, c)
			gc, ok := c.(*GRPCChecker)
			require.True(t, ok, `"grpc" factory should return *GRPCChecker`)
			assert.Equal(t, tt.wantTLS, gc.useTLS)
		})
	}
}

// TestGRPCChecker_InvalidTLSNoValueEcho 钉住安全契约（docs/07 §5）：
// Extra["tls"] 的非法值构造期错误不得回显 Extra 原值（可能含敏感信息），
// 模式对齐 TestHTTPChecker_InvalidExpectedStatusNoValueEcho 先例。
func TestGRPCChecker_InvalidTLSNoValueEcho(t *testing.T) {
	const secret = "s3cr3t"
	c, err := checker.NewChecker("grpc", checker.TargetConfig{
		Addr:  "example:9000",
		Extra: map[string]string{"tls": secret},
	})
	require.Error(t, err)
	assert.Nil(t, c)
	assert.ErrorIs(t, err, ErrInvalidTLSValue)
	assert.NotContains(t, err.Error(), secret, "Extra value must not be echoed into the error message (docs/07 sec. 5)")
	assert.Equal(t, "checker: invalid tls value", err.Error(),
		"sentinel returned directly without wrapping (message is self-contained, docs/06 sec. 5)")
}

// TestProtocolsRegistered_Grpc 验证本包 init() 已完成 "grpc" 协议注册
// （内部测试二进制链接本包即触发 init；blank import 消费者的同一机制）。
func TestProtocolsRegistered_Grpc(t *testing.T) {
	assert.Contains(t, checker.Protocols(), "grpc")

	// 注册结果可经 NewChecker 实际构造。
	c, err := checker.NewChecker("grpc", checker.TargetConfig{Addr: "example:9000"})
	require.NoError(t, err)
	assert.NotNil(t, c)
}
