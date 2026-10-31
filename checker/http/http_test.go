package http

import (
	"context"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serverAddr 从 httptest.Server 的 URL 中提取 host:port 形式的 Addr。
func serverAddr(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	return strings.TrimPrefix(ts.URL, "http://")
}

func TestHTTPChecker_StatusCodes(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		wantHealthy bool
	}{
		{name: "200_healthy_by_default", status: nethttp.StatusOK, wantHealthy: true},
		{name: "301_healthy_default_range_no_redirect_follow", status: nethttp.StatusMovedPermanently, wantHealthy: true},
		{name: "404_unhealthy", status: nethttp.StatusNotFound, wantHealthy: false},
		{name: "500_unhealthy", status: nethttp.StatusInternalServerError, wantHealthy: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
				w.WriteHeader(tt.status)
			}))
			defer ts.Close()

			c, err := checker.NewChecker("http", checker.TargetConfig{Addr: serverAddr(t, ts)})
			require.NoError(t, err)
			require.NotNil(t, c)

			result, err := c.Check(context.Background())
			require.NoError(t, err, "builtin Checker failure paths return facts; the error channel stays nil (M-5)")
			require.NotNil(t, result)

			assert.Equal(t, tt.wantHealthy, result.Healthy)
			assert.Equal(t, tt.status, result.StatusCode)
			assert.Greater(t, result.Latency, time.Duration(0))
			assert.True(t, result.Timestamp.IsZero(), "builtin Checker leaves Timestamp zero (M-6)")

			if tt.wantHealthy {
				assert.NoError(t, result.Err)
			} else {
				require.Error(t, result.Err)
				assert.Contains(t, result.Err.Error(), "status")
			}
		})
	}
}

func TestHTTPChecker_ExpectedStatus(t *testing.T) {
	tests := []struct {
		name         string
		serverStatus int
		wantHealthy  bool
	}{
		{name: "404_matches_expected_healthy", serverStatus: nethttp.StatusNotFound, wantHealthy: true},
		{name: "200_not_expected_unhealthy", serverStatus: nethttp.StatusOK, wantHealthy: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
				w.WriteHeader(tt.serverStatus)
			}))
			defer ts.Close()

			c, err := checker.NewChecker("http", checker.TargetConfig{
				Addr:  serverAddr(t, ts),
				Extra: map[string]string{"expected_status": "404"},
			})
			require.NoError(t, err)

			result, err := c.Check(context.Background())
			require.NoError(t, err)
			require.NotNil(t, result)

			assert.Equal(t, tt.wantHealthy, result.Healthy, "exact match when expected_status is specified (G4)")
			assert.Equal(t, tt.serverStatus, result.StatusCode)
		})
	}
}

func TestHTTPChecker_DefaultPathMethod(t *testing.T) {
	type reqInfo struct {
		method string
		path   string
	}
	got := make(chan reqInfo, 1)
	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		got <- reqInfo{method: r.Method, path: r.URL.Path}
		w.WriteHeader(nethttp.StatusOK)
	}))
	defer ts.Close()

	c, err := checker.NewChecker("http", checker.TargetConfig{Addr: serverAddr(t, ts)})
	require.NoError(t, err)

	result, err := c.Check(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Healthy)

	select {
	case info := <-got:
		assert.Equal(t, "GET", info.method, "default method is GET")
		assert.Equal(t, "/health", info.path, "default path is /health")
	case <-time.After(2 * time.Second):
		t.Fatal("handler received no request (select timeout prevents deadlock)")
	}
}

func TestHTTPChecker_ConnectionRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	c, err := checker.NewChecker("http", checker.TargetConfig{Addr: addr})
	require.NoError(t, err)

	result, err := c.Check(context.Background())
	require.NoError(t, err, "connection refused takes the factual-return path (result, nil) (M-5)")
	require.NotNil(t, result)
	assert.False(t, result.Healthy)
	assert.Error(t, result.Err)
	assert.Equal(t, 0, result.StatusCode)
}

func TestHTTPChecker_CancelledContext(t *testing.T) {
	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		w.WriteHeader(nethttp.StatusOK)
	}))
	defer ts.Close()

	c, err := checker.NewChecker("http", checker.TargetConfig{Addr: serverAddr(t, ts)})
	require.NoError(t, err)

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

	// result 或 error 形态均可（任务卡口径），但必须非健康。
	if checkErr != nil {
		return
	}
	require.NotNil(t, result)
	assert.False(t, result.Healthy)
	assert.Error(t, result.Err)
}

// TestHTTPChecker_ConnectionReuseWithBody 钉住连接复用契约（2026-10-30 审计 P2-2 修复，
// defect-audit-r2 + bench-coverage-eval §3 实证）：handler 返回非空小 body 时，
// Checker 必须在 Close 前有界 drain body，使连接归还 DefaultTransport idle 池。
// 修复前 body 未读即 Close 走 net/http earlyClose 路径弃连接——任意大小 body
// 均触发（2 字节同样不复用），3 次探测新建 3 条 TCP 连接（复用为零）。
// 计数 oracle：httptest.Server ConnState hook 统计 StateNew（连接建立先于
// RoundTrip 返回，Check 结束即计数完整，无异步窗口）。
func TestHTTPChecker_ConnectionReuseWithBody(t *testing.T) {
	var newConns atomic.Int32
	ts := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		w.WriteHeader(nethttp.StatusOK)
		_, _ = w.Write([]byte("OK\n")) // 非空小 body：修复前触发 earlyClose 弃连接
	}))
	ts.Config.ConnState = func(_ net.Conn, state nethttp.ConnState) {
		if state == nethttp.StateNew {
			newConns.Add(1)
		}
	}
	ts.Start()
	defer ts.Close()

	c, err := checker.NewChecker("http", checker.TargetConfig{Addr: serverAddr(t, ts)})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		result, checkErr := c.Check(context.Background())
		require.NoError(t, checkErr)
		require.NotNil(t, result)
		require.True(t, result.Healthy)
	}

	assert.Equal(t, int32(1), newConns.Load(),
		"3 sequential probes against a non-empty-body endpoint must reuse a single connection (bounded drain returns it to the idle pool)")
}

func TestHTTPChecker_FactoryErrors(t *testing.T) {
	t.Run("empty_addr", func(t *testing.T) {
		c, err := checker.NewChecker("http", checker.TargetConfig{Addr: ""})
		require.Error(t, err)
		assert.Nil(t, c)
		assert.Contains(t, err.Error(), "empty target address")
		assert.ErrorIs(t, err, checker.ErrEmptyTargetAddress) // 三个子包共用的 sentinel（docs/06 §5）
	})

	t.Run("invalid_expected_status", func(t *testing.T) {
		c, err := checker.NewChecker("http", checker.TargetConfig{
			Addr:  "127.0.0.1:80",
			Extra: map[string]string{"expected_status": "abc"},
		})
		require.Error(t, err)
		assert.Nil(t, c)
		assert.Contains(t, err.Error(), "invalid expected_status")
		assert.ErrorIs(t, err, ErrInvalidExpectedStatus) // sentinel 身份（docs/06 §5.1 规则 8 双契约）
	})
}

// TestHTTPChecker_InvalidExpectedStatusNoValueEcho 钉住安全契约（docs/07 §5）：
// Extra["expected_status"] 可能含密码/secret，非法值的构造期错误不得回显 Extra 原值，
// 也不得 wrap strconv 解析错误（其消息本身含原值，属二次泄露）。
func TestHTTPChecker_InvalidExpectedStatusNoValueEcho(t *testing.T) {
	const secret = "s3cr3t"
	c, err := checker.NewChecker("http", checker.TargetConfig{
		Addr:  "127.0.0.1:80",
		Extra: map[string]string{"expected_status": secret},
	})
	require.Error(t, err)
	assert.Nil(t, c)
	assert.ErrorIs(t, err, ErrInvalidExpectedStatus)
	assert.NotContains(t, err.Error(), secret, "Extra value must not be echoed into the error message (docs/07 sec. 5)")
	assert.Equal(t, "checker: invalid expected_status", err.Error(),
		"sentinel returned directly without wrapping strconv (message is self-contained, docs/06 sec. 5)")
}

// TestHTTPChecker_ExpectedStatusRangeValidation 是 2026-10-29 审计 P2-2 修复（数值范围校验）的
// 表驱动钉死：expected_status 合法口径为 [100,599]（HTTP 状态码全集），
// 恰合法边界（100/599）接受，越界拒绝——显式 "0"/负值与 "0=未设置" 哨兵冲突
// （静默回退区间判定 = 配置被吞），越界正值恒不匹配（永久失败无诊断线索），
// 均违反 fail-fast 原则（docs/05 §5：不静默钳制或回退默认值）。
// 拒绝路径沿用安全契约：sentinel 直返、不回显 Extra 值（docs/07 §5，
// 与 TestHTTPChecker_InvalidExpectedStatusNoValueEcho 的非数字 token 路径同型）。
func TestHTTPChecker_ExpectedStatusRangeValidation(t *testing.T) {
	tests := []struct {
		name  string
		value string
		valid bool // true = 恰合法（必须接受）；false = 越界（必须拒绝）
	}{
		{name: "explicit_zero_rejected", value: "0", valid: false},
		{name: "negative_rejected", value: "-1", valid: false},
		{name: "below_range_99_rejected", value: "99", valid: false},
		{name: "lower_bound_100_accepted", value: "100", valid: true},
		{name: "upper_bound_599_accepted", value: "599", valid: true},
		{name: "above_range_600_rejected", value: "600", valid: false},
		{name: "far_above_range_999_rejected", value: "999", valid: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := checker.NewChecker("http", checker.TargetConfig{
				Addr:  "127.0.0.1:80",
				Extra: map[string]string{"expected_status": tt.value},
			})
			if tt.valid {
				require.NoError(t, err, "boundary value in [100,599] must be accepted")
				require.NotNil(t, c)
				return
			}
			require.Error(t, err, "out-of-range expected_status must be rejected fail-fast (no silent fallback to range matching)")
			assert.Nil(t, c)
			assert.ErrorIs(t, err, ErrInvalidExpectedStatus, "the existing sentinel is reused (no new API)")
			assert.NotContains(t, err.Error(), tt.value, "the configured value must not be echoed into the error message (docs/07 sec. 5)")
			assert.Equal(t, "checker: invalid expected_status", err.Error(),
				"sentinel returned directly without wrapping strconv (message is self-contained, docs/06 sec. 5)")
		})
	}
}
