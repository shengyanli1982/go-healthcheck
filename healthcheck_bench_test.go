package healthcheck

// 基准测试套件（根包）——设计 SSOT：.agent-work-b652f803/specs/2026-10-29-benchmark-design.md。
// 覆盖热路径 1（probeFn 主动探测全周期）、热路径 2（读路径）、热路径 3（被动上报库侧段）
// 与零分配守护测试（docs/08 §8 承诺的程序化落点）。
//
// 方法学要点（设计 §2/§3 裁定，违反即方法学缺陷）：
//   - M3：setup 完成后 b.ResetTimer() 是唯一合法计时起点；禁止显式重启计时；
//     teardown 一律在 b.StopTimer() 之后（框架在基准函数返回时才停表，循环后代码会被计时）。
//   - M4：全部 b.RunParallel 基准为「聚合吞吐口径」（aggregate throughput semantics），
//     与单线程 ns/op 不可直接横比（逐基准注释标注）。
//   - M8：基准内不写共享可变全局；benchChecker/benchClock 为只读共享，合法；
//     读路径经指针接收者 + mutex 加锁副作用不可能被 DCE，不加 DCE sink（§3.4）。
//   - §3.1：不复用既有 fakeChecker/fakeScheduler（记录结构无界增长污染测量），
//     本文件新建 bench 专用替身；共享 CheckResult 的 Timestamp 必须预置非零
//     （safeCheck 仅 IsZero 才补填 → 非零则不写共享对象，RunParallel 下无 race）。
//   - §3.2：probeFn 同包直调 seam；同 host 并发直调违反 SF1 单写者不变式，禁止；
//     跨 host 并发（ParallelHosts）合法。
//   - §3.5：翻转类路径分段测量、报告合成（detector 段归 passive 包 FlipCycle，
//     库侧段归本包 Report_FlipWithCallback），合成值非单次实测。
//   - §3.6：注入 benchClock（固定时间，无锁）使被动窗口永不过期；不注入带锁 fakeClock。
//
// 口径纪律：断言消息英文、注释中文（docs/08 §1.1）；Benchmark 骨架允许 stdlib，
// 断言仍走 testify require。

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/shengyanli1982/go-healthcheck/passive"
	"github.com/stretchr/testify/require"
)

// ———————————————————————————— bench 专用测试替身（设计 §3.1） ————————————————————————————

// benchFixedTime 是 bench 替身的固定时间戳。
// Timestamp 非零 → safeCheck 免补填（healthcheck.go 仅 IsZero 才写共享 result）→
// 跨迭代无写入污染、RunParallel 下无 race（设计陷阱裁定 (2)）。
var benchFixedTime = time.Unix(1700000000, 0)

// benchErrUnhealthy 是 benchBadResult 的事实化错误（M-5 形态：结果对象携带 Err）。
var benchErrUnhealthy = errors.New("bench: unhealthy")

// 共享预置探测结果（只读、发布后不可变 R-d；ApplyResult 只存指针不改写，共享合法）。
var (
	benchOKResult  = &checker.CheckResult{Healthy: true, Latency: time.Millisecond, Timestamp: benchFixedTime}
	benchBadResult = &checker.CheckResult{Healthy: false, Latency: time.Millisecond, Err: benchErrUnhealthy, Timestamp: benchFixedTime}
)

// benchChecker 是基准专用只读 Checker 替身：恒返回共享 benchOKResult。
// 无锁、无分配、无记录结构（与 fakeChecker 的无界 deadlines 切片相对）；
// 只读共享合法（M8），可跨 host / 跨 goroutine 复用同一实例。
type benchChecker struct{}

// 编译期断言：满足 Checker 接口。
var _ checker.Checker = benchChecker{}

// Check 返回共享预置成功结果（Timestamp 非零，safeCheck 不写共享对象）。
func (benchChecker) Check(context.Context) (*checker.CheckResult, error) {
	return benchOKResult, nil
}

// benchAltChecker 是交替返回 ok/bad 两个预置结果的 Checker 替身
// （rise=1/fall=1 时每轮驱动状态机翻转）。
// n 计数非原子：仅限单线程基准（设计 §3.1）；n 单调性兼作防调用消失守卫。
type benchAltChecker struct {
	n int
}

// 编译期断言：满足 Checker 接口。
var _ checker.Checker = (*benchAltChecker)(nil)

// Check 按调用次数交替返回 benchOKResult / benchBadResult。
func (c *benchAltChecker) Check(context.Context) (*checker.CheckResult, error) {
	r := benchBadResult
	if c.n%2 == 0 {
		r = benchOKResult
	}
	c.n++
	return r, nil
}

// noopScheduler 是全空 hostScheduler 替身：隔离 workqueue，测 probeFn 纯本体；
// 与 FullCycle（真实调度器）的 ns/op 差分 = workqueue Done/Rearm 集成成本（设计 §3.1）。
type noopScheduler struct{}

// 编译期断言：满足 hostScheduler 接口（6 方法）。
var _ hostScheduler = noopScheduler{}

// Schedule 空实现（C2 机械同步：签名收 any 预装箱值）。
func (noopScheduler) Schedule(any, time.Duration, time.Duration) {}

// Cancel 空实现（C2 机械同步）。
func (noopScheduler) Cancel(any) {}

// Run 空实现。
func (noopScheduler) Run(context.Context, int, func(string)) {}

// Shutdown 空实现，返回 nil（幂等安全）。
func (noopScheduler) Shutdown(context.Context) error { return nil }

// Done 空实现（C2 机械同步）。
func (noopScheduler) Done(any) {}

// Rearm 空实现（C2 机械同步）。
func (noopScheduler) Rearm(any, time.Duration) {}

// benchClock 是固定时间时钟替身：无锁、无 atomic，只读共享合法（M8）。
// 被动窗口永不过期 → steady 路径稳定（设计 §3.6；不复用带锁 fakeClock，锁成本污染测量）。
type benchClock struct{}

// 编译期断言：满足 Clock 接口。
var _ Clock = benchClock{}

// Now 返回固定时间常量。
func (benchClock) Now() time.Time { return benchFixedTime }

// alwaysFlipDetector 是恒翻转的 PassiveDetector stub：Record 恒返回 (true, !last)，
// 隔离 detector 判定段（归 passive 包 BenchmarkDetector_Record_FlipCycle），
// 专测 report 的库侧段 cbMu→hostIdentity→CurrentHealthyReader 断言→复核→
// SetPassiveHealthy→notifyLocked（段合成原则 §3.5；r4 P1-1 修复后库侧段
// 含投递前复核，本 stub 实现可选接口使守护/基准覆盖真实修复路径）。
// 内部 bool 非原子：仅限单线程基准，禁 RunParallel。
type alwaysFlipDetector struct {
	last bool // 当前被动镜像值，初值 true（与 HostState.passiveHealthy 初值对齐）
}

// 编译期断言：满足 PassiveDetector 接口与可选复核接口。
var (
	_ passive.PassiveDetector      = (*alwaysFlipDetector)(nil)
	_ passive.CurrentHealthyReader = (*alwaysFlipDetector)(nil)
)

// newAlwaysFlipDetector 构造初始镜像为 true 的恒翻转 stub
// （初值必须与 HostState.passiveHealthy 初值 true 对齐，首轮 Record 才产生综合翻转）。
func newAlwaysFlipDetector() *alwaysFlipDetector {
	return &alwaysFlipDetector{last: true}
}

// Record 恒翻转：返回 (true, !last) 并更新内部镜像。
func (d *alwaysFlipDetector) Record(string, bool) (bool, bool) {
	d.last = !d.last
	return true, d.last
}

// CurrentHealthy 返回与内部镜像一致的当前值（ok 恒 true）：复核替换后
// newHealthy 与 Record 快照同值，行为不变——守护对象是断言+复核调用本身
// 的零分配，而非替换语义（替换语义由回归测试钉死）。
func (d *alwaysFlipDetector) CurrentHealthy(string) (bool, bool) {
	return d.last, true
}

// Reset 空实现（stub 无窗口状态需要清理）。
func (alwaysFlipDetector) Reset(string) {}

// noopCallback 是空状态回调：注册 notifyLocked 的 defer/recover 路径但不做事
// （回调投递成本计入翻转类基准，投递消失由计数型回调守卫，见 Report_FlipWithCallback）。
func noopCallback(string, bool, *CheckResult) {}

// ———————————————————————————— bench setup 辅助 ————————————————————————————

// benchHostName 生成基准 host 名（%04d 定宽，GetHealthyHosts 字典序断言友好）。
func benchHostName(i int) string { return fmt.Sprintf("bench-host-%04d", i) }

// benchHostNames 预生成 host 名切片：setup 期构造，防测量循环内逐轮 Sprintf 分配。
func benchHostNames(n int) []string {
	hosts := make([]string, n)
	for i := 0; i < n; i++ {
		hosts[i] = benchHostName(i)
	}
	return hosts
}

// newBenchManagerTB 构造基准/守护测试用 manager：默认注入 benchClock（固定时间，
// 窗口永不过期）+ 真实调度器（TimerQueue）；TB 版供 Benchmark 与守护测试共用。
// 调用方负责 teardown：m.Stop 回收 New() 构造期启动的 TimerQueue 搬运 goroutine。
func newBenchManagerTB(tb testing.TB, opts ...Option) *HealthManager {
	tb.Helper()
	m, err := New(append([]Option{WithClock(benchClock{})}, opts...)...)
	require.NoError(tb, err, "manager construction must succeed in benchmark setup")
	require.NotNil(tb, m)
	return m
}

// addBenchHostsTB 注册 n 个基准 host：benchChecker + interval=1h。
// interval=1h 关键设计（陷阱裁定 (3)）：Rearm 的 PutAfter(1h) 基准期间永不到期，
// 幂等模式同值重复 Put 仅替换堆节点（workqueue putAtInternal 同值查重）→ 堆大小恒定；
// Done 对非 processing 值是安全 no-op（timerqueue Done 契约）。
func addBenchHostsTB(tb testing.TB, m *HealthManager, n int) {
	tb.Helper()
	for i := 0; i < n; i++ {
		require.NoError(tb,
			m.AddHost(benchHostName(i), WithChecker(benchChecker{}), WithHostInterval(time.Hour)),
			"AddHost must succeed in benchmark setup")
	}
}

// ———————————————————————————— 热路径 1：probeFn 全周期 ————————————————————————————

// BenchmarkProbeFn_FullCycle 测热路径 1 全周期：hostParams 单次查找 →
// context.WithTimeout → safeCheck → cbMu → ApplyResult → [flip 时 notifyLocked] →
// Done/Rearm（真实 timerQueueScheduler，含 workqueue 集成成本）。
//
// 口径：单线程串行直调 probeFn（§3.2 seam；同 host 并发违反 SF1 禁测）；
// ns/op 含真实 WithTimeout 分配与 workqueue 锁；allocs/op 报告不设零断言
// （生产真实路径有分配，基线记录后回归阈值 ±20% B/op）。
func BenchmarkProbeFn_FullCycle(b *testing.B) {
	b.Run("steady", func(b *testing.B) {
		m := newBenchManagerTB(b, WithWorkers(1), WithOnStateChange(noopCallback), WithRise(2), WithFall(3))
		require.NoError(b, m.AddHost("bench-host", WithChecker(benchChecker{}), WithHostInterval(time.Hour)))
		ctx := context.Background()
		// 预热直调 probeFn rise=2 次达 UP（steady 稳态：riseCount++ 无翻转）
		m.probeFn(ctx, "bench-host")
		m.probeFn(ctx, "bench-host")
		require.True(b, m.IsHealthy("bench-host"), "preheat must bring host UP")
		require.NotNil(b, m.GetLastResult("bench-host"), "preheat must publish lastResult")

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.probeFn(ctx, "bench-host")
		}
		b.StopTimer()
		require.NoError(b, m.Stop(context.Background()), "Stop must reclaim the TimerQueue goroutine")
	})

	b.Run("flip", func(b *testing.B) {
		alt := &benchAltChecker{}
		m := newBenchManagerTB(b, WithWorkers(1), WithOnStateChange(noopCallback), WithRise(1), WithFall(1))
		require.NoError(b, m.AddHost("bench-host", WithChecker(alt), WithHostInterval(time.Hour)))
		ctx := context.Background()
		// 预热 1 次达 UP（rise=1）；此后每轮 ok/bad 交替 → 每轮翻转 + notifyLocked
		m.probeFn(ctx, "bench-host")
		require.True(b, m.IsHealthy("bench-host"), "preheat must bring host UP (rise=1)")
		nBefore := alt.n

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.probeFn(ctx, "bench-host")
		}
		b.StopTimer()
		// 失真守卫：benchAltChecker.n 单调恰好推进 b.N（防编译器消调用/路径被跳过）
		require.Equal(b, b.N, alt.n-nBefore, "alternating checker must be invoked exactly once per iteration")
		require.NoError(b, m.Stop(context.Background()), "Stop must reclaim the TimerQueue goroutine")
	})
}

// BenchmarkProbeFn_CoreOnly 与 FullCycle(steady) 成对运行、成对入报告：
// sched 替换为 noopScheduler → 两者 ns/op 差分 = workqueue Done/Rearm 集成成本
// （derived metric，热路径 1 与 5 的解耦点）。接线按 newTestManager 既有模式：
// 替换后立即 Shutdown 真实调度器防 goroutine 泄漏。
func BenchmarkProbeFn_CoreOnly(b *testing.B) {
	m := newBenchManagerTB(b, WithWorkers(1), WithOnStateChange(noopCallback), WithRise(2), WithFall(3))
	real := m.sched
	m.sched = noopScheduler{}
	require.NoError(b, real.Shutdown(context.Background()), "real scheduler must shut down cleanly after swap")

	require.NoError(b, m.AddHost("bench-host", WithChecker(benchChecker{}), WithHostInterval(time.Hour)))
	ctx := context.Background()
	m.probeFn(ctx, "bench-host")
	m.probeFn(ctx, "bench-host")
	require.True(b, m.IsHealthy("bench-host"), "preheat must bring host UP")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.probeFn(ctx, "bench-host")
	}
	b.StopTimer()
	require.NoError(b, m.Stop(context.Background()), "Stop with noop scheduler must be idempotent-safe")
}

// BenchmarkProbeFn_ParallelHosts 测多 host 并发探测：manager.mu.RLock（hostParams）
// 争用 + per-host cbMu 分散度 + HostState.mu 分布。
//
// 口径：b.RunParallel = 聚合吞吐语义（M4），与单线程 ns/op 不可横比；
// atomic 轮转计数成本（~20ns）计入，占比 <5%（probeFn 周期 μs 级）；
// 同 host 偶发重叠探测依赖幂等 Done no-op 语义安全、无状态破坏
// （ApplyResult 幂等计数除外——口径噪声已在设计 §9.2(5) 标注）。
// 争用判读：与 FullCycle(steady) 的 ns/op 比值 + R3 mutex profile 联合归因；
// atomic 争用经 R1 CPU profile -peek 'atomic' 帧归因（M4）。
func BenchmarkProbeFn_ParallelHosts(b *testing.B) {
	h := 4 * runtime.GOMAXPROCS(0)
	m := newBenchManagerTB(b, WithWorkers(1), WithOnStateChange(noopCallback), WithRise(2), WithFall(3))
	addBenchHostsTB(b, m, h)
	hosts := benchHostNames(h)
	ctx := context.Background()
	// 串行预热各 host 达 UP（SF1：同 host probeFn 并发直调禁止，预热必须单线程）
	for i := 0; i < h; i++ {
		m.probeFn(ctx, hosts[i])
		m.probeFn(ctx, hosts[i])
	}
	require.True(b, m.IsHealthy(hosts[0]), "preheat must bring host UP")

	var idx atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.probeFn(ctx, hosts[int(idx.Add(1)%uint64(h))])
		}
	})
	b.StopTimer()
	require.Equal(b, h, m.Hosts(), "host registry must be intact after parallel probing")
	require.NoError(b, m.Stop(context.Background()), "Stop must reclaim the TimerQueue goroutine")
}

// ———————————————————————————— 热路径 2：读路径 ————————————————————————————

// BenchmarkIsHealthy 测热路径 2：getManagedHost RLock map 查找 + HostState 自锁读；
// map 规模（1/100/1000）对查找成本的影响。
//
// 口径：单线程；不加 DCE sink（§3.4：指针接收者 + mutex 加锁副作用不可消除）。
// allocs/op == 0 硬守护（TestZeroAllocGuards_ReadAndReportPaths）；
// docs/08 §8「O(1) 纳秒级」：三档 hosts 的 ns/op 应近似平坦，
// 1000-host 显著劣化 = map 探测异常，报告标注。
func BenchmarkIsHealthy(b *testing.B) {
	for _, n := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("hosts-%d", n), func(b *testing.B) {
			m := newBenchManagerTB(b)
			addBenchHostsTB(b, m, n)
			target := benchHostName(n / 2) // 查询目标 = 中位 host
			require.Equal(b, n, m.Hosts(), "setup must register all hosts")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m.IsHealthy(target)
			}
			b.StopTimer()
			require.NoError(b, m.Stop(context.Background()), "Stop must reclaim the TimerQueue goroutine")
		})
	}
}

// BenchmarkIsHealthy_Parallel 测 RWMutex 读锁并发扩展性（多读者）+ HostState.mu 读争用。
//
// 口径：b.RunParallel = 聚合吞吐语义（M4）标注；allocs/op == 0（守护测试覆盖）；
// 并发/单核 ns/op 比值随 GOMAXPROCS 应次线性（读锁共享），显著劣化 → R3 mutex profile 归因。
func BenchmarkIsHealthy_Parallel(b *testing.B) {
	const n = 1000
	m := newBenchManagerTB(b)
	addBenchHostsTB(b, m, n)
	hosts := benchHostNames(n)
	require.Equal(b, n, m.Hosts(), "setup must register all hosts")

	var idx atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.IsHealthy(hosts[int(idx.Add(1)%uint64(n))])
		}
	})
	b.StopTimer()
	require.NoError(b, m.Stop(context.Background()), "Stop must reclaim the TimerQueue goroutine")
}

// BenchmarkGetHealthyHosts 测热路径 2：1000-host RLock 遍历 + 逐 host 自锁读 +
// 锁外 sort.Strings（C4 字典序契约）。
//
// 口径：单线程；快照/控制面 API 定位（「不建议每请求热路径调用」）——
// 本基准量化该建议的依据；B/op ≈ 16KB（make([]string,0,1000)）+ sort 无额外分配，
// 报告不设零断言；docs/08 §8 目标 O(n log n) 量级核对。
func BenchmarkGetHealthyHosts(b *testing.B) {
	b.Run("all-up", func(b *testing.B) { benchGetHealthyHosts(b, 1000, 1) })
	b.Run("half-up", func(b *testing.B) { benchGetHealthyHosts(b, 1000, 2) })
}

// benchGetHealthyHosts 注册 total 个 host，其中每 upEvery 个预热一个至 UP
// （走公开 probeFn seam，不直写 mh.hs）；upEvery=1 → all-up，upEvery=2 → half-up（偶数 index）。
func benchGetHealthyHosts(b *testing.B, total, upEvery int) {
	b.Helper()
	m := newBenchManagerTB(b, WithOnStateChange(noopCallback))
	addBenchHostsTB(b, m, total)
	ctx := context.Background()
	for i := 0; i < total; i += upEvery {
		host := benchHostName(i)
		m.probeFn(ctx, host) // rise=2（默认）：两次成功达 UP
		m.probeFn(ctx, host)
		require.True(b, m.IsHealthy(host), "preheat must bring host UP")
	}
	want := (total + upEvery - 1) / upEvery
	got := m.GetHealthyHosts()
	require.Len(b, got, want, "setup sanity: healthy host count")
	require.True(b, sort.StringsAreSorted(got), "setup sanity: C4 lexicographic contract")

	var last []string
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		last = m.GetHealthyHosts()
	}
	b.StopTimer()
	require.Len(b, last, want, "healthy host count must stay stable across iterations")
	require.True(b, sort.StringsAreSorted(last), "C4: result must be sorted lexicographically")
	require.NoError(b, m.Stop(context.Background()), "Stop must reclaim the TimerQueue goroutine")
}

// BenchmarkGetLastResult 测热路径 2：RLock 查找 + 自锁读指针。
// allocs/op == 0 硬守护（TestZeroAllocGuards_ReadAndReportPaths）。
func BenchmarkGetLastResult(b *testing.B) {
	const n = 1000
	m := newBenchManagerTB(b)
	addBenchHostsTB(b, m, n)
	target := benchHostName(n / 2)
	m.probeFn(context.Background(), target) // 预热一轮：lastResult 非 nil
	require.Same(b, benchOKResult, m.GetLastResult(target), "preheat must publish the shared preset result")

	var last *CheckResult
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		last = m.GetLastResult(target)
	}
	b.StopTimer()
	require.Same(b, benchOKResult, last, "returned pointer must equal the preheat-written shared result")
	require.NoError(b, m.Stop(context.Background()), "Stop must reclaim the TimerQueue goroutine")
}

// BenchmarkHosts 测热路径 2：RLock + len。allocs/op == 0 硬守护（守护测试覆盖）。
func BenchmarkHosts(b *testing.B) {
	const n = 1000
	m := newBenchManagerTB(b)
	addBenchHostsTB(b, m, n)

	var last int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		last = m.Hosts()
	}
	b.StopTimer()
	require.Equal(b, n, last, "host count must equal the registered hosts")
	require.NoError(b, m.Stop(context.Background()), "Stop must reclaim the TimerQueue goroutine")
}

// ———————————————————————————— 热路径 3：被动上报 ————————————————————————————

// BenchmarkReportSuccess_SteadyState 测热路径 3 最热子路径（宿主每请求上报）：
// manager.mu.RLock 查找 → detector.Record success（count=0 无翻转）→ early return。
//
// 口径：单线程；benchClock 固定时间 → 窗口永不过期（steady 路径稳定，§3.6）。
// allocs/op == 0 硬守护（docs/08 §8「ReportSuccess/Failure O(1) 零分配」的直接验收；
// TestZeroAllocGuards_ReadAndReportPaths AllocsPerRun(1000)）；
// ns/op 量级 = 纳秒级（两把无争用锁 + map 查找）。
func BenchmarkReportSuccess_SteadyState(b *testing.B) {
	m := newBenchManagerTB(b) // 默认构造真实 TimeWindowDetector（注入 benchClock）
	addBenchHostsTB(b, m, 1)
	host := benchHostName(0)
	m.ReportSuccess(host) // 预热 1 次：惰性建 hostWindow（首建分配不入测量段）

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.ReportSuccess(host)
	}
	b.StopTimer()
	require.NoError(b, m.Stop(context.Background()), "Stop must reclaim the TimerQueue goroutine")
}

// BenchmarkReportFailure_AlreadyDown 测已判死 host 的失败上报稳态
// （生产常态：max_fails 判死后宿主仍持续上报）：Record → count++ →
// count>=maxFails && passiveHealthy==false → 无翻转 early return。
//
// 口径：单线程；count 随 b.N 增长（int 无溢出风险）。allocs/op == 0 硬守护；
// 窗口内 count++ 段（未判死）成本归 passive 包基准（段合成原则 §3.5），根级不重复测。
func BenchmarkReportFailure_AlreadyDown(b *testing.B) {
	m := newBenchManagerTB(b) // 默认 detector：maxFails=3
	addBenchHostsTB(b, m, 1)
	host := benchHostName(0)
	// setup 期喂 3 次 ReportFailure 完成翻转（不计时段）
	m.ReportFailure(host)
	m.ReportFailure(host)
	m.ReportFailure(host)
	require.False(b, m.IsHealthy(host), "host must be down after maxFails failures")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.ReportFailure(host)
	}
	b.StopTimer()
	require.NoError(b, m.Stop(context.Background()), "Stop must reclaim the TimerQueue goroutine")
}

// BenchmarkReport_FlipWithCallback 测 report 的库侧翻转段：cbMu 获取 →
// hostIdentity 指针复核 → CurrentHealthyReader 断言→复核 →
// SetPassiveHealthy 镜像写 → notifyLocked 回调投递。
//
// 口径：单线程（alwaysFlipDetector 内部 bool 非原子，禁 RunParallel）；
// 段合成口径标注（§3.5）：与 passive 包 BenchmarkDetector_Record_FlipCycle 之和
// ≈ 端到端翻转成本，合成值、非单次实测。
// allocs/op 报告不设零守护（defer+recover 注册的 open-coded defer 预期零分配，
// 保守先记录基线；实测恒 0 可在后续版本升级为守护）。
func BenchmarkReport_FlipWithCallback(b *testing.B) {
	cbCalls := 0
	counting := func(string, bool, *CheckResult) { cbCalls++ }
	m := newBenchManagerTB(b, WithWorkers(1), WithPassiveDetector(newAlwaysFlipDetector()), WithOnStateChange(counting))
	addBenchHostsTB(b, m, 1)
	host := benchHostName(0)
	ctx := context.Background()
	// 预热至 activeHealthy=true（rise=2 默认）：被动镜像翻转才能驱动综合翻转 + 回调投递
	m.probeFn(ctx, host)
	m.probeFn(ctx, host)
	require.True(b, m.IsHealthy(host), "preheat must bring host UP")
	cbBefore := cbCalls

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.ReportSuccess(host) // stub detector 恒翻转 → 每轮走完整 cbMu→SetPassiveHealthy→notify 段
	}
	b.StopTimer()
	// 防投递消失守卫：回调调用计数 == b.N
	require.Equal(b, b.N, cbCalls-cbBefore, "state-change callback must fire exactly once per flip")
	require.NoError(b, m.Stop(context.Background()), "Stop must reclaim the TimerQueue goroutine")
}

// BenchmarkReportSuccess_Parallel 测宿主多 goroutine 上报争用面：
// manager.mu.RLock（共享）+ detector leaf lock（单全局 mutex）+
// steady 不取 cbMu（same-host 时 cbMu 无争用）。
//
// 口径：b.RunParallel = 聚合吞吐语义（M4）标注；detector 全局单锁是已知争用点，
// 本基准量化其代价：same-host vs sharded 差值 = leaf lock 争用成本；
// R3 mutex profile 应显示 TimeWindowDetector.mu 为 same-host 主争用帧（归因交叉验证）。
// allocs/op == 0（守护测试同路径覆盖）。
func BenchmarkReportSuccess_Parallel(b *testing.B) {
	b.Run("same-host", func(b *testing.B) { benchReportSuccessParallel(b, 1) })
	b.Run("sharded", func(b *testing.B) { benchReportSuccessParallel(b, 1000) })
}

// benchReportSuccessParallel 注册 n 个 host（预热建窗）后 RunParallel 上报 success；
// n=1 → same-host（leaf lock 全争用），n=1000 → sharded（轮转分散）。
func benchReportSuccessParallel(b *testing.B, n int) {
	b.Helper()
	m := newBenchManagerTB(b) // 真实 TimeWindowDetector + benchClock
	addBenchHostsTB(b, m, n)
	hosts := benchHostNames(n)
	for i := 0; i < n; i++ {
		m.ReportSuccess(hosts[i]) // 预热：惰性建窗（首建分配不入测量段）
	}

	var idx atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.ReportSuccess(hosts[int(idx.Add(1)%uint64(n))])
		}
	})
	b.StopTimer()
	require.NoError(b, m.Stop(context.Background()), "Stop must reclaim the TimerQueue goroutine")
}

// ———————————————————————————— 方法学标定：共享轮转计数器 floor ————————————————————————————

// BenchmarkAtomicIdxFloor 是空计数器标定基准（出处：2026-10-30 基准方法学评估 P1 强制前置修补）：
// 标定 ns 级并发基准共享轮转计数器的 cache line 争用 floor，
// 供 IsHealthy_Parallel 等并发基准的争用归因做扣除基线。
//
// 背景：4 个 ns 级并发基准（IsHealthy_Parallel、ReportSuccess_Parallel/{same-host,sharded}、
// passive 包 Detector_Record_Parallel/{same-host,sharded-hosts}，次级 ProbeFn_ParallelHosts）
// 共用局部 `var idx atomic.Uint64` 轮转 host——GOMAXPROCS 路并发争用同一 cache line，
// 该成本此前未标定，在 ns 级基准中会冒充被测锁争用（如 IsHealthy_Parallel 151ns vs
// 单线程 23ns 的扩展性归因，必须先扣除本 floor 再判读 RWMutex 争用占比）。
//
// 口径：无任何被测调用，仅测 b.RunParallel 下 idx.Add(1) 空循环成本；
// M4 聚合吞吐语义；allocs/op == 0（atomic.Add 不分配）；
// floor 随 GOMAXPROCS 变化，标定时须与被标定的并发基准同 -cpu 档运行
// （基准输出天然带 -N 后缀区分）；root 包一处标定即可——passive 包
// Detector_Record_Parallel 为同形态计数器，两包基准同机同 GOMAXPROCS 跑。
func BenchmarkAtomicIdxFloor(b *testing.B) {
	var idx atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx.Add(1)
		}
	})
}

// ———————————————————————————— 零分配守护测试（M5，设计 §6.10） ————————————————————————————

// TestZeroAllocGuards_ReadAndReportPaths 是根包零分配承诺的程序化落点：
// Benchmark 函数本身无失败钩子，守护用 testing.AllocsPerRun(1000, f) == 0 硬断言
// （N=1000 防整除截断假象，M5）。docs/08 §8：IsHealthy O(1) 纳秒级、
// ReportSuccess/Failure O(1) 零分配。前置 setup 与对应基准一致
// （预热惰性建窗、benchClock 固定时间、真实 detector）。
// C2 顺带（pprof 报告不做清单 #10 移交项）：ReportSuccess 翻转段
// （cbMu→hostIdentity→CurrentHealthyReader 断言→复核→
// SetPassiveHealthy→notifyLocked，含 defer+recover 注册）
// 升级为守护——独立 manager 注入 alwaysFlipDetector 恒翻转 + 计数回调，
// 与 BenchmarkReport_FlipWithCallback 同形态（基线实测恒 0 allocs）。
func TestZeroAllocGuards_ReadAndReportPaths(t *testing.T) {
	m := newBenchManagerTB(t, WithOnStateChange(noopCallback))
	t.Cleanup(func() { _ = m.Stop(context.Background()) })
	ctx := context.Background()

	// 读路径 host：预热至 UP + lastResult 非 nil（与 BenchmarkGetLastResult setup 一致）
	require.NoError(t, m.AddHost("bench-read", WithChecker(benchChecker{}), WithHostInterval(time.Hour)))
	m.probeFn(ctx, "bench-read")
	m.probeFn(ctx, "bench-read")
	require.True(t, m.IsHealthy("bench-read"), "preheat must bring host UP")
	require.Same(t, benchOKResult, m.GetLastResult("bench-read"), "lastResult must be the shared preset pointer")

	// ReportSuccess steady host：预热 1 次惰性建窗（timewindow 首建分配不入守护段）
	require.NoError(t, m.AddHost("bench-rs", WithChecker(benchChecker{}), WithHostInterval(time.Hour)))
	m.ReportSuccess("bench-rs")

	// ReportFailure already-down host：3 次失败翻转为被动 DOWN（默认 maxFails=3）
	require.NoError(t, m.AddHost("bench-rf", WithChecker(benchChecker{}), WithHostInterval(time.Hour)))
	m.ReportFailure("bench-rf")
	m.ReportFailure("bench-rf")
	m.ReportFailure("bench-rf")
	require.False(t, m.IsHealthy("bench-rf"), "host must be down after maxFails failures")

	// 翻转段 host（C2 守护）：独立 manager——alwaysFlipDetector 每轮 Record 恒翻转，
	// 计数回调与 BenchmarkReport_FlipWithCallback 同形态；预热至 UP（rise=2 默认）
	// 使被动镜像翻转驱动综合翻转 + notifyLocked 投递。
	flipM := newBenchManagerTB(t, WithWorkers(1),
		WithPassiveDetector(newAlwaysFlipDetector()), WithOnStateChange(func(string, bool, *CheckResult) {}))
	t.Cleanup(func() { _ = flipM.Stop(context.Background()) })
	require.NoError(t, flipM.AddHost("bench-flip", WithChecker(benchChecker{}), WithHostInterval(time.Hour)))
	flipM.probeFn(ctx, "bench-flip")
	flipM.probeFn(ctx, "bench-flip")
	require.True(t, flipM.IsHealthy("bench-flip"), "preheat must bring flip host UP")

	guarded := map[string]func(){
		"IsHealthy":                    func() { m.IsHealthy("bench-read") },
		"GetLastResult":                func() { m.GetLastResult("bench-read") },
		"Hosts":                        func() { m.Hosts() },
		"ReportSuccess(steady)":        func() { m.ReportSuccess("bench-rs") },
		"ReportFailure(already-down)":  func() { m.ReportFailure("bench-rf") },
		"ReportSuccess(flip-callback)": func() { flipM.ReportSuccess("bench-flip") },
	}
	for name, f := range guarded {
		allocs := testing.AllocsPerRun(1000, f)
		require.Zero(t, allocs, "zero-alloc guard failed for %s", name)
	}
}
