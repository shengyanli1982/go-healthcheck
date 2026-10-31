package http

// 基准测试套件（checker/http）——设计 SSOT：.agent-work-b652f803/specs/2026-10-29-benchmark-design.md §6.6。
// 覆盖热路径 7：HTTPChecker.Check（NewRequestWithContext + DefaultTransport.RoundTrip
// 连接池复用 + 状态判定）。
//
// 隔离策略：httptest.NewServer（stdlib，loopback 真实 IO）；handler 默认返回
// 空 body（200 或 503），最小化 server 侧成本；with-body 变体返回非空小 body，
// 覆盖有界 drain 连接复用路径（2026-10-30 审计 P2-2，生产 /health 端点的代表形态）。
// 口径标注：含 loopback TCP + net/http 全栈，非纯 CPU 成本，不与内存型基准横比；
// 单线程串行（RunParallel 会使 httptest server 成为共同瓶颈，测不到 checker 本体，
// 不设并发档）。

import (
	"context"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/stretchr/testify/require"
)

// serverAddrTB 从 httptest.Server 的 URL 提取 host:port 形式的 Addr
// （TB 版供 Benchmark 复用；与 http_test.go 既有 serverAddr(t) 并存，零改动既有文件）。
func serverAddrTB(tb testing.TB, ts *httptest.Server) string {
	tb.Helper()
	return strings.TrimPrefix(ts.URL, "http://")
}

// BenchmarkHTTPChecker_Check 测热路径 7：HTTP 探测全栈成本。
// ok-200 = 健康判定路径；unhealthy-503 = 失败判定 + fmt.Errorf error 分配
// （生产死节点常态路径）；两者 B/op 差值 = error 路径分配成本（报告标注 derived）。
// ok-200-with-body = 非空小 body + 有界 drain 连接复用路径（2026-10-30 审计 P2-2 修复后的
// 生产代表形态：/health 端点普遍返回内容，drain 使连接真实归还 idle 池）。
func BenchmarkHTTPChecker_Check(b *testing.B) {
	b.Run("ok-200", func(b *testing.B) {
		benchHTTPCheckerCheck(b, nethttp.StatusOK, true, "")
	})
	b.Run("unhealthy-503", func(b *testing.B) {
		benchHTTPCheckerCheck(b, nethttp.StatusServiceUnavailable, false, "")
	})
	b.Run("ok-200-with-body", func(b *testing.B) {
		benchHTTPCheckerCheck(b, nethttp.StatusOK, true, "OK\n")
	})
}

// benchHTTPCheckerCheck 以固定状态码 handler 起 httptest.Server，
// 经同包 factory 直构 HTTPChecker 串行探测 b.N 次。
// body 非空时 handler 附带小 body（with-body 变体，暴露 drain 复用路径）。
// B/op、ns/op 基线记录（含请求分配，不设零断言）。
func benchHTTPCheckerCheck(b *testing.B, status int, wantHealthy bool, body string) {
	b.Helper()
	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		w.WriteHeader(status) // 状态码先行；空 body 变体到此为止，最小化 server 侧成本
		if body != "" {
			_, _ = w.Write([]byte(body)) // 非空小 body：客户端须 drain 才复用连接（2026-10-30 审计 P2-2）
		}
	}))
	b.Cleanup(ts.Close)

	c, err := newHTTPChecker(checker.TargetConfig{Addr: serverAddrTB(b, ts), Timeout: 5 * time.Second})
	require.NoError(b, err, "http checker construction must succeed")
	require.NotNil(b, c)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, checkErr := c.Check(ctx)
		// 循环内失真防护：防连接失败静默测 error 路径（设计 §6.6）
		if checkErr != nil {
			b.Fatalf("check returned infrastructure error: %v", checkErr)
		}
		if wantHealthy && !res.Healthy {
			b.Fatalf("probe failed: %v", res.Err)
		}
		if !wantHealthy && res.Healthy {
			b.Fatal("probe unexpectedly healthy on failure-status server")
		}
	}
	b.StopTimer()
}
