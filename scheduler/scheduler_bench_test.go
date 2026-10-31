package scheduler

// 基准测试套件（scheduler 包）——设计 SSOT：.agent-work-b652f803/specs/2026-10-29-benchmark-design.md §6.4。
// 边界重申：测本库 scheduler 封装面（Schedule/Cancel/Done/Rearm/Run/Shutdown 的
// 集成成本），不测 workqueue 内部算法（§1.2：第三方库自带 bench）。
// 测量调用一律走封装方法；同包可经 s.tq/s.bq 做 teardown 校验（本套件以
// Shutdown 返回 nil 作为无泄漏判据，不直接触内部字段）。
//
// 方法学：M3（ResetTimer 唯一计时起点、禁止显式重启计时、StopTimer 后 teardown）；
// §3.3：RunThroughput 的探针计数是被测吞吐的一部分（worker 汇聚点），单 atomic
// 计数器（~20ns vs 调度周期 μs 量级，占比 <5%），报告标注「计数成本含内」，
// 用 W1 单元（无争用）交叉核对。

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// BenchmarkSched_DoneRearm 测热路径 5：probeFn 收尾段
// （Done 对非 processing 值安全 no-op + Rearm PutAfter 幂等入堆）。
//
// 口径：单线程循环 { Done; Rearm(1h) }；interval=1h 永不到期 + PutAfter 同值
// 替换堆节点（workqueue putAtInternal 查重）→ 堆恒定 1 元素；不 Run
// （无 worker，GetWithContext 不被消费，在队项滞留是预期稳态）。
// C2 生产形态修订：host 以变量一次装箱后复用（原字面量形态被编译期静态装箱，
// 掩盖真实分配——pprof 报告 §4(4) 证伪「0 allocs」为伪影）；生产探测热路径
// 恒为变量 host，改造后本基准读数才代表生产形态。
// allocs/op == 0（elementpool 全复用 + wrapper 透传不装箱）；
// 与根包 FullCycle vs CoreOnly 差分交叉验证。
func BenchmarkSched_DoneRearm(b *testing.B) {
	s, err := New()
	require.NoError(b, err, "scheduler construction must succeed")
	hostStr := "bench-host"        // 变量形态：装箱不可被编译期静态化
	var host any = hostStr         // 调用方一次装箱，测量循环复用（C2 契约形态）
	s.Schedule(host, time.Hour, 0) // 一次入队（offset=0 立即投递入内层队列，无 worker 消费）

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Done(host)             // 非 processing 值 → 安全 no-op（timerqueue Done 契约）
		s.Rearm(host, time.Hour) // PutAfter(1h) 永不到期；同值替换 → 堆恒定
	}
	b.StopTimer()
	require.NoError(b, s.Shutdown(context.Background()), "Shutdown must return nil (no worker leak)")
}

// BenchmarkSched_ScheduleCancel 测 AddHost/RemoveHost 的调度段
// （PutAfter 入堆 + Cancel 堆删除，即封装面 Schedule/Cancel 的完整生产路径）。
//
// setup 说明：offset 取 1h（未来时刻）使 Schedule 真正入堆、Cancel 命中堆删除——
// 设计 §6.4 测量目标列为「PutAfter + 堆删除」；offset=0 会立即投递入内层队列，
// Cancel（仅删堆）永不命中，与测量目标矛盾，故按目标列取未来 offset（偏差已登记）。
// host 名轮转 1000 个（设计字面保留；未来 offset + 每轮 Cancel 删除下无幂等残留）。
// C2 生产形态修订：装箱在 setup 期一次完成（boxed []any），测量循环复用——
// 原 string 变量直传形态每 op 装箱 2 次（pprof §4(4) 行级钉死 :71/:77 各 16B）。
// 口径：单线程；allocs/op == 0（elementpool 回收堆节点 + wrapper 透传不装箱）。
func BenchmarkSched_ScheduleCancel(b *testing.B) {
	s, err := New()
	require.NoError(b, err, "scheduler construction must succeed")
	const pool = 1000
	boxed := make([]any, pool) // setup 期预生成 + 一次装箱，防测量循环内 Sprintf/装箱分配
	for i := 0; i < pool; i++ {
		boxed[i] = fmt.Sprintf("bench-h-%04d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := boxed[i%pool]
		s.Schedule(h, time.Hour, time.Hour) // PutAfter(1h)：入堆（同值替换，堆至多 1 元素）
		s.Cancel(h)                         // 堆删除（front 命中 O(1)；底层 false 是正常路径 SF3）
	}
	b.StopTimer()
	require.NoError(b, s.Shutdown(context.Background()), "Shutdown must return nil (no worker leak)")
}

// BenchmarkSched_RunThroughput 测热路径 5 端到端 + docs/08 §8「探测吞吐
// workers x hosts」矩阵：GetWithContext pop → probe（atomic 计数 + Done + Rearm(1ms)）
// 完整 worker 循环。
//
// 口径：吞吐口径、非 ns/op——正式基线以 -benchtime=1x 单发运行（设计 §6.4/§8），
// b.ReportMetric 输出 probes/s；ns/op ≈ 墙钟窗口、allocs/op 含 worker goroutine
// 窗口内全部分配，两者仅作记录不作判读。周期下限受 TimerQueue 毫秒精度约束
// （R-b）：读数为「含定时器精度的端到端调度吞吐」。W=12 单元在 <12 核机器上
// 为过订阅口径（报告标注）。
// 验收：计数 > 0（防空跑）；Shutdown 在 5s 内返回 nil（无 worker 泄漏）；
// 矩阵结果应随 W 单调不降（H 固定）——报告级核对，反常 → R1 CPU profile 归因。
func BenchmarkSched_RunThroughput(b *testing.B) {
	for _, w := range []int{1, 4, 12} {
		for _, h := range []int{10, 1000} {
			b.Run(fmt.Sprintf("W%d-H%d", w, h), func(b *testing.B) {
				benchSchedRunThroughput(b, w, h)
			})
		}
	}
}

// benchSchedRunThroughput 单格矩阵：workers x hosts，固定墙钟窗口内计数探测吞吐。
func benchSchedRunThroughput(b *testing.B, workers, hosts int) {
	b.Helper()
	s, err := New()
	require.NoError(b, err, "scheduler construction must succeed")

	var probes atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	// C2 生产形态修订：host 装箱在 setup 期一次完成（boxed map），worker 回调复用
	// 装箱值调 Done/Rearm（原 string 直传形态每探测装箱 2 次）。map 必须在 Run
	// 之前构建完毕——offset=0 立即投递，回调读取与 setup 写入并发即 map 竞态；
	// 只读查找零分配，成本 ~10ns/探测（vs 周期 ~ms 级）计入吞吐口径，占比 <1%。
	boxed := make(map[string]any, hosts)
	for i := 0; i < hosts; i++ {
		name := fmt.Sprintf("bench-host-%04d", i)
		boxed[name] = name
	}
	s.Run(ctx, workers, func(host string) {
		probes.Add(1) // 计数成本含内（§3.3）：单 atomic ~20ns，占比 <5%，W1 单元交叉核对
		h := boxed[host]
		s.Done(h)
		s.Rearm(h, time.Millisecond) // 固定 1ms 重排（毫秒精度下限 R-b，口径含定时器精度）
	})
	for i := 0; i < hosts; i++ {
		name := fmt.Sprintf("bench-host-%04d", i)
		s.Schedule(boxed[name], time.Millisecond, 0)
	}

	// 墙钟窗口自适应：正式基线 -benchtime=1x 单发（b.N==1）→ 5s 完整窗口（设计 §6.4）；
	// 冒烟/回归多迭代（b.N>1）按比例缩短单轮窗口，防 6 格 × N × 5s 总时长失控
	// （框架 run1 预热亦为 N=1 → 5s，属正式口径的一部分）。
	wall := 5 * time.Second / time.Duration(b.N)
	if wall < 10*time.Millisecond {
		wall = 10 * time.Millisecond
	}

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		time.Sleep(wall)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	cancel()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	require.NoError(b, s.Shutdown(shutCtx), "Shutdown must return nil within 5s (no worker leak)")
	count := probes.Load()
	require.Greater(b, count, int64(0), "probe counter must be positive (empty-run guard)")
	b.ReportMetric(float64(count)/elapsed.Seconds(), "probes/s")
}
