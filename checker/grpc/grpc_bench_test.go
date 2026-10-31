package grpc

// 基准测试套件（checker/grpc）——设计 SSOT：.agent-work-b652f803/specs/2026-10-29-benchmark-design.md §6.8。
// 覆盖热路径 7：GRPCChecker.Check 的完整 per-probe dial 成本——每次探测新建
// grpc.NewClient + Health/Check RPC + conn.Close（「无跨探测复用」设计的价格量化，
// 三个 IO 基准中价值最高，为「是否引入连接复用」决策提供数据）。
//
// 隔离策略：bufconn（零新增依赖：google.golang.org/grpc/test/bufconn 在 grpc
// 模块内）+ passthrough:///bufnet target（grpc_test.go 既有裁定：默认 dns resolver
// 无法解析 "bufnet"，passthrough 直通 WithContextDialer）。
// 注入点：GRPCChecker.dialOpts 同包字段（测试注入 seam；生产为 nil）。
//
// 口径标注：bufconn 内存管道，无真实 socket/TLS/DNS；含 HTTP/2 握手 + SETTINGS
// 交换 + 一次 unary RPC + 连接销毁；单线程串行；外推生产延迟须加网络段（§9.2）。

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

// newBufServerTB 启动 bufconn 上的 grpc.Server（TB 版供 Benchmark 复用：
// 复制 grpc_test.go 既有 newBufServer(t) 逻辑改 t.Cleanup→tb.Cleanup，
// 零改动既有文件，设计 §4 约束）；withHealth=true 时注册官方 health.NewServer()
// 并返回其句柄。服务器停止由 tb.Cleanup 托管。
func newBufServerTB(tb testing.TB, withHealth bool) (*bufconn.Listener, *health.Server) {
	tb.Helper()
	lis := bufconn.Listen(bufSize)
	srv := grpclib.NewServer()
	var hs *health.Server
	if withHealth {
		hs = health.NewServer()
		healthpb.RegisterHealthServer(srv, hs)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	tb.Cleanup(func() {
		srv.Stop()
		err := <-serveErr
		if !errors.Is(err, grpclib.ErrServerStopped) {
			assert.NoError(tb, err, "bufconn grpc.Server.Serve exited abnormally")
		}
	})
	return lis, hs
}

// BenchmarkGRPCChecker_Check 测热路径 7：gRPC Health/Check 探测。
// serving = SERVING 判定路径；not-serving = NOT_SERVING 判定 + status error 分配；
// 两者 B/op 差值 = 失败路径分配（报告标注 derived）。
func BenchmarkGRPCChecker_Check(b *testing.B) {
	b.Run("serving", func(b *testing.B) {
		benchGRPCCheckerCheck(b, healthpb.HealthCheckResponse_SERVING, true)
	})
	b.Run("not-serving", func(b *testing.B) {
		benchGRPCCheckerCheck(b, healthpb.HealthCheckResponse_NOT_SERVING, false)
	})
}

// benchGRPCCheckerCheck 以指定整体服务状态起 bufconn server，直构 GRPCChecker
// （dialOpts 注入 bufconn dialer）串行探测 b.N 次。
// B/op、ns/op 基线记录（grpc 客户端分配较重，不设零断言）。
func benchGRPCCheckerCheck(b *testing.B, serving healthpb.HealthCheckResponse_ServingStatus, wantHealthy bool) {
	b.Helper()
	lis, hs := newBufServerTB(b, true)
	require.NotNil(b, hs, "health server handle must be available")
	hs.SetServingStatus("", serving) // service=""：整体检查（newBufChecker 默认口径）

	c := &GRPCChecker{
		addr:     bufTarget, // passthrough:///bufnet 直通 bufconn dialer（grpc_test.go 既有裁定）
		timeout:  5 * time.Second,
		dialOpts: bufDialOpts(lis), // 同包 dialOpts 注入 seam（生产为 nil）
	}

	ctx := context.Background()
	// setup 期 sanity 一次：RPC 层状态恒 codes.OK（SERVING/NOT_SERVING 均为成功应答）
	res, err := c.Check(ctx)
	require.NoError(b, err, "M-5: builtin Checker failure paths return facts (result, nil)")
	require.NotNil(b, res)
	require.Equal(b, wantHealthy, res.Healthy, "sanity: serving status mapping")
	require.Equal(b, int(codes.OK), res.StatusCode, "sanity: RPC-level status must be codes.OK")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, checkErr := c.Check(ctx)
		// 循环内失真防护：防连接异常静默测 error 路径（设计 §6.8）
		if checkErr != nil {
			b.Fatalf("check returned infrastructure error: %v", checkErr)
		}
		if wantHealthy && !res.Healthy {
			b.Fatalf("check failed: %v", res.Err)
		}
		if !wantHealthy && res.Healthy {
			b.Fatal("check unexpectedly healthy on NOT_SERVING server")
		}
	}
	b.StopTimer()
}
