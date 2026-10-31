package tcp

// 基准测试套件（checker/tcp）——设计 SSOT：.agent-work-b652f803/specs/2026-10-29-benchmark-design.md §6.7。
// 覆盖热路径 7：TCPChecker.Check（DialContext 建连 + 即关 / dial ECONNREFUSED
// 事实化失败返回）。
//
// 隔离策略：本地 loopback net.Listen("tcp", "127.0.0.1:0")（同 tcp_test.go 既有模式）
// + accept-and-close goroutine。
//
// 运维约束（关键）：checker 侧 conn.Close() 主动关闭 → 客户端 ephemeral port 进入
// TIME_WAIT；b.N 无上限时可能耗尽端口（macOS 默认 ephemeral 范围 ≈16k）。缓解三条：
// (1) 运行命令强制 -benchtime=5000x 上限（§8，轮间建议冷却 30s）；
// (2) accept 循环 Accept 后立即 Close（server 先发 FIN，使客户端 close 趋向被动
//     关闭、加速端口回收）；
// (3) 循环内失真断言兜底捕获端口耗尽导致的 dial 失败。
//
// 口径标注：connect 档含内核 loopback TCP 握手 syscall，非纯用户态成本；单线程串行。

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/stretchr/testify/require"
)

// BenchmarkTCPChecker_Check 测热路径 7：TCP 建连探测。
// connect = DialContext 建连 + 即关；refused = 死节点失败路径（ECONNREFUSED
// 事实化 error 返回，无 TIME_WAIT 累积，benchtime 可默认）。
// 两者 ns/op 对比 = 失败路径成本（通常更快，报告记录）。
func BenchmarkTCPChecker_Check(b *testing.B) {
	b.Run("connect", func(b *testing.B) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(b, err, "loopback listener must start")
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				conn, err := ln.Accept()
				if err != nil {
					return // listener 已关闭（b.Cleanup），正常退出
				}
				_ = conn.Close() // 立即关闭：server 先发 FIN，加速客户端 ephemeral port 回收
			}
		}()
		b.Cleanup(func() {
			_ = ln.Close()
			<-done // 泄漏防护：等待 accept goroutine 退出
		})

		c, err := newTCPChecker(checker.TargetConfig{Addr: ln.Addr().String(), Timeout: 5 * time.Second})
		require.NoError(b, err, "tcp checker construction must succeed")
		require.NotNil(b, c)

		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res, checkErr := c.Check(ctx)
			if checkErr != nil {
				b.Fatalf("check returned infrastructure error: %v", checkErr)
			}
			// 循环内失真防护：端口耗尽/accept 异常导致的 dial 失败在此兜底捕获
			if !res.Healthy {
				b.Fatalf("dial failed: %v", res.Err)
			}
		}
		b.StopTimer()
	})

	b.Run("refused", func(b *testing.B) {
		// setup：listen → 取端口 → close（端口无主，期望 connection refused）+
		// 预验证 dial 钉住 refused 形态：极低概率该端口被其他进程重绑定 → 基准
		// 静默测到成功路径（测量失真而非显式失败）。预验证判据：dial 必须立即
		// ECONNREFUSED；dial 成功（重绑定方内核完成握手）或超时/其他非 refused
		// 形态 → 换端口重试（上限 3 轮），仍不可用 → b.Fatalf 显式失败并说明原因。
		const maxSetupAttempts = 3
		addr := ""
		for attempt := 1; attempt <= maxSetupAttempts && addr == ""; attempt++ {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(b, err, "loopback listener must start")
			candidate := ln.Addr().String()
			require.NoError(b, ln.Close(), "listener must close to leave the addr unlistened")

			conn, dialErr := net.DialTimeout("tcp", candidate, time.Second)
			if dialErr == nil {
				_ = conn.Close() // 端口已被重绑定（握手被完成）→ 换端口重试
				continue
			}
			if !errors.Is(dialErr, syscall.ECONNREFUSED) {
				continue // 超时或其他非 refused 失败 → 同样不可用于本档，换端口重试
			}
			addr = candidate // 预验证通过：该端口当前即时 refused
		}
		if addr == "" {
			b.Fatalf("refused setup: no immediately-refused loopback port after %d attempts (candidate ports likely rebound by other processes)", maxSetupAttempts)
		}

		c, err := newTCPChecker(checker.TargetConfig{Addr: addr, Timeout: 5 * time.Second})
		require.NoError(b, err, "tcp checker construction must succeed")
		require.NotNil(b, c)

		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res, checkErr := c.Check(ctx)
			if checkErr != nil {
				b.Fatalf("check returned infrastructure error: %v", checkErr)
			}
			if res.Err == nil {
				b.Fatal("expected refused connection error")
			}
		}
		b.StopTimer()
	})
}
