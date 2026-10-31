// Package grpc 提供 gRPC 健康检查 Checker 子包（SF6 依赖隔离）。
//
// 消费者以 blank import 完成 "grpc" 协议注册（对齐 database/sql 驱动模式）：
//
//	import _ "github.com/shengyanli1982/go-healthcheck/checker/grpc"
//
// 主 checker 包不依赖 google.golang.org/grpc；未 import 本子包的用户
// 依赖树保持极简（DoD #5 依赖审计门禁）。
package grpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// ErrInvalidTLSValue 由 "grpc" 协议工厂在 Extra["tls"] 含非法值（非精确字面量
// "true"/"false"，如 "True"/"1"/"yes" 或显式空串）时返回（构造期失败，不进入
// 探测）。修复 r4 审计 #P3-3：非精确字面量原先静默降级为 insecure 明文探测
// （安全方向配置误用静默失败），现 fail-fast 拒绝，对齐 checker/http 对
// expected_status 的构造期校验风格（ErrInvalidExpectedStatus 先例）。
// 依 docs/07 §5 安全约束，Extra 值不进入 error 消息（可能含敏感信息），
// 故消息仅含键名、不回显具体值。消息自包含 "checker: " 前缀（docs/06 §5，
// 不造子包前缀变体）；消费者可经 errors.Is(err, grpc.ErrInvalidTLSValue) 判别。
var ErrInvalidTLSValue = errors.New("checker: invalid tls value")

// GRPCChecker 按 gRPC Health Checking Protocol（grpc.health.v1 Health/Check）探测目标健康状态。
//
// 健康判定与 StatusCode（gRPC codes.Code 整数值）映射（docs/09 §5.3，R-c 裁定）：
//
//	| 探测结果                     | Healthy | StatusCode                 | Err                 |
//	|------------------------------|---------|----------------------------|---------------------|
//	| SERVING                      | true    | 0  (codes.OK)              | nil                 |
//	| NOT_SERVING                  | false   | 0  (codes.OK)              | 含 "NOT_SERVING"    |
//	| NOT_FOUND（service 未设置）  | false   | 5  (codes.NotFound)        | RPC 错误            |
//	| UNIMPLEMENTED（无 health 服务）| false | 12 (codes.Unimplemented)   | RPC 错误            |
//	| 连接失败/超时/取消           | false   | 对应 codes.Code            | RPC 错误            |
//
// 连接策略：每次探测新建 grpc.NewClient 连接，探测结束即关闭（无跨探测复用）。
// TLS：默认 insecure（本地探测场景居多，docs/07 §2.3）；Extra["tls"] 仅接受
// 精确字面量 "true"（启用系统信任链 TLS，credentials.NewClientTLSFromCert）
// 与 "false"（insecure），任何其他值在构造期返回 ErrInvalidTLSValue
// （fail-fast，r4 审计 #P3-3：不静默降级）。
// 服务名：Extra["service"] 指定，默认 ""（检查服务器整体）。
//
// 作为内置 Checker，失败路径统一事实化返回（M-5）：
// (*CheckResult{Healthy:false, Err:err}, nil)，不占用 (nil, error) 通道；
// Timestamp 保持零值（M-6，由根包 safeCheck 以注入 Clock 补齐）。
//
// 消费者经 blank import 注册后由 checker.NewChecker("grpc", cfg) 构造，
// 不直接实例化本类型。
type GRPCChecker struct {
	addr     string
	timeout  time.Duration
	service  string               // Extra["service"]，默认 ""（检查服务器整体）
	useTLS   bool                 // Extra["tls"] 精确 "true"/"false" 解析结果；键缺失默认 insecure（docs/07 §2.3，构造期校验见 ErrInvalidTLSValue）
	dialOpts []grpclib.DialOption // 测试注入 seam（bufconn dialer）；生产为 nil
}

func init() {
	// blank import 触发注册（对齐 database/sql 驱动模式）。
	// 协议名取 checker.ProtocolGRPC（注册名单一事实源，固定非空）、factory 非 nil，
	// Register 不会失败（零日志，丢弃返回值）。
	_ = checker.Register(checker.ProtocolGRPC, newGRPCChecker)
}

// newGRPCChecker 是 "grpc" 协议的 CheckerFactory（docs/02 §9 内置工厂命名）。
// 解析 Extra["service"]（默认 ""，检查服务器整体）与 Extra["tls"]：仅接受精确
// 字面量 "true"（启用 TLS）/"false"（insecure），键缺失默认 insecure（现状
// 不变）；任何其他值（含 "True"/"1"/"yes" 等非精确字面量与显式空串）直返
// ErrInvalidTLSValue（fail-fast，不静默降级；不回显 Extra 值，docs/07 §5）。
// Addr 为空时返回 error（构造期失败，不进入探测）。
func newGRPCChecker(cfg checker.TargetConfig) (checker.Checker, error) {
	if cfg.Addr == "" {
		return nil, checker.ErrEmptyTargetAddress // 共享 sentinel 直返（"checker/grpc: " 前缀变体已取消，三子包消息统一，docs/06 §5）
	}
	// comma-ok 区分「键缺失」与「键存在但值非法」：前者走默认 insecure（现状），
	// 后者 fail-fast（r4 审计 #P3-3：安全方向配置误用不得静默失败）。
	// 大小写敏感精确匹配，无 "1"/"yes"/"True" 等变体宽容——宽容即静默降级通道。
	useTLS := false
	if v, ok := cfg.Extra["tls"]; ok {
		switch v {
		case "true":
			useTLS = true
		case "false":
			useTLS = false
		default:
			return nil, ErrInvalidTLSValue // sentinel 直返（不回显 Extra 值，docs/07 §5）
		}
	}
	return &GRPCChecker{
		addr:    cfg.Addr,
		timeout: cfg.Timeout,
		service: cfg.Extra["service"],
		useTLS:  useTLS,
	}, nil
}

// Check 执行一次 Health/Check RPC 并返回事实化结果。
// 尊重 ctx 取消/超时；timeout>0 时自包 context.WithTimeout（仅服务 standalone
// 使用场景——GRPCChecker 为导出类型，消费者可脱离 manager 单独使用；库路径恒被
// 外层 probeCtx 先触发：两者时长恒等且内层创建更晚，deadline 严格更晚，OE-1）；
// Latency 为 RPC 调用耗时；结果不填 Timestamp（M-6，由根包 safeCheck 补齐）。
func (c *GRPCChecker) Check(ctx context.Context) (*checker.CheckResult, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout) // standalone 语义依赖，禁止删除（OE-1 裁定）
		defer cancel()
	}

	creds := insecure.NewCredentials()
	if c.useTLS {
		creds = credentials.NewClientTLSFromCert(nil, "")
	}
	// dialOpts 追加在新建切片之后，不别名、不修改 c.dialOpts 底层数组。
	opts := append([]grpclib.DialOption{grpclib.WithTransportCredentials(creds)}, c.dialOpts...)
	conn, err := grpclib.NewClient(c.addr, opts...)
	if err != nil {
		// NewClient 仅做参数/目标校验（惰性连接），失败即探测器无法执行，仍事实化返回（M-5）。
		return &checker.CheckResult{Healthy: false, StatusCode: 0, Err: err}, nil
	}
	defer conn.Close()

	start := time.Now()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: c.service})
	if err != nil {
		// NOT_FOUND / UNIMPLEMENTED / 连接失败 / 超时 / 取消统一事实化返回（R-c 裁定：
		// NOT_FOUND/UNIMPLEMENTED → Healthy=false）；StatusCode = int(codes.Code)。
		st, _ := status.FromError(err)
		return &checker.CheckResult{
			Healthy:    false,
			StatusCode: int(st.Code()),
			Latency:    time.Since(start),
			Err:        err,
		}, nil
	}

	healthy := resp.Status == healthpb.HealthCheckResponse_SERVING
	result := &checker.CheckResult{
		Healthy:    healthy,
		StatusCode: int(codes.OK),
		Latency:    time.Since(start),
	}
	if !healthy {
		// NOT_SERVING / UNKNOWN 等非 SERVING 应答：RPC 成功（StatusCode=OK），判定不健康。
		result.Err = fmt.Errorf("grpc health status: %s", resp.Status)
	}
	return result, nil
}
