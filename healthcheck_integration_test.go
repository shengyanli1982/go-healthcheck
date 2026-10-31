package healthcheck

// ST-08 集成层测试：M2 C1-C10 集成侧断言（docs/08 §3.4，fakeChecker+fakeClock+真实 Scheduler 端到端）。
// 复用 healthcheck_basic_test.go 的 fake 辅助（fakeClock/fakeChecker/fakeScheduler/
// fakePassiveDetector/cbCollector/sinkCollector/newTestManager，不重复定义）；
// 本文件仅新增 basic 替身不具备能力的局部替身（运行时开关、重叠探测）。
//
// 真实 Scheduler 场景纪律：先 Start（Run 启动 workers）再 AddHost——探测消费依赖
// 已运行的 workers，且错峰 offset 以 AddHost 时刻为锚（R-a），先启动使观测窗口可控。
// 纯 seam 场景（probeFn 直调）注入 fakeScheduler：确定性控制调度，与真实调度器解耦
// （R3 后真实调度器未 Run 时 Shutdown 为立即关闭 + 有界等待即刻返回）。
//
// 断言纪律（docs/08 §1.1）：require=前置断言、assert=业务断言；顺序敏感断言用
// assert.Equal（禁 ElementsMatch）；非测试 goroutine 内只用 assert。
// 确定性承诺为事件序列级（SF5）：只断言次数/相对顺序，不断言调度时刻。

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/shengyanli1982/go-healthcheck/passive"
)

// ———————————————————————————— 集成测试局部辅助 ————————————————————————————

// waitFor 以 10ms 步进轮询 cond 直到其返回 true 或 timeout 耗尽，返回是否满足。
// 仅供测试 goroutine 使用（结果由调用方 require/assert 断言）。
func waitFor(t *testing.T, cond func() bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// switchableChecker 是可原子切换健康/不健康的 Checker 替身（C1 专用：
// 先恒 true 后恒 false）。basic_test 的 fakeChecker 只支持预编程结果序列，
// 不支持运行中切换，故新增本替身（非重复定义）。
type switchableChecker struct {
	healthy atomic.Bool
	calls   atomic.Int32
}

// 编译期断言：满足 Checker 接口。
var _ checker.Checker = (*switchableChecker)(nil)

// Check 返回当前开关值（恒成功，仅 Healthy 随开关变化）。
func (c *switchableChecker) Check(ctx context.Context) (*checker.CheckResult, error) {
	c.calls.Add(1)
	return &checker.CheckResult{Healthy: c.healthy.Load()}, nil
}

// setHealthy 原子切换后续探测结果。
func (c *switchableChecker) setHealthy(v bool) { c.healthy.Store(v) }

// callCount 返回已发生的 Check 调用次数。
func (c *switchableChecker) callCount() int { return int(c.calls.Load()) }

// overlapDetectorChecker 是 C9 单写者不重叠探测替身：Check 内 enter/exit 计数
// 严格配对，任一时刻并发在内者 >1 即记 violation（进入/退出序号严格配对的
// 集成侧等价断言，M2-C9）。附带 2ms 探测耗时放大潜在重叠窗口。
type overlapDetectorChecker struct {
	active     atomic.Int32
	violations atomic.Int32
	calls      atomic.Int32
}

// 编译期断言：满足 Checker 接口。
var _ checker.Checker = (*overlapDetectorChecker)(nil)

// Check 记录进入/退出配对；并发 >1 记 violation。
func (c *overlapDetectorChecker) Check(ctx context.Context) (*checker.CheckResult, error) {
	if n := c.active.Add(1); n > 1 {
		c.violations.Add(1)
	}
	defer c.active.Add(-1)
	c.calls.Add(1)
	time.Sleep(2 * time.Millisecond) // 探测耗时相对 interval(10ms) 不可忽略，放大重叠窗口
	return &checker.CheckResult{Healthy: true}, nil
}

// ———————————————————————————— M2 C1-C10 集成断言 ————————————————————————————

// TestIntegration_C1_RiseFallEndToEnd 是 M2-C1 集成侧：真实 Scheduler 端到端，
// rise=2/fall=2，fakeChecker 先恒 true 后恒 false（原子开关切换）。
// 事件序列级断言（SF5，不断言时刻）：(true) 恰在 ≥2 次成功探测后出现；
// 切 false 后 (false) 在 ≥2 次失败探测后出现；全程恰 2 个翻转事件。
func TestIntegration_C1_RiseFallEndToEnd(t *testing.T) {
	fc := newFakeClock()
	col := &cbCollector{}
	chk := &switchableChecker{}
	chk.setHealthy(true)
	m := newTestManager(t, nil, // 真实 Scheduler（New 内置 timerQueueScheduler，不替换 m.sched）
		WithInterval(20*time.Millisecond), WithClock(fc),
		WithRise(2), WithFall(2),
		WithPassiveDetector(newFakePassiveDetector()), // 隔离被动路径，专注主动 rise/fall
		WithOnStateChange(col.onChange))

	// workqueue 陷阱纪律：先 Start（workers 就绪）再 AddHost
	require.NoError(t, m.Start(context.Background()))
	const host = "c1:80"
	require.NoError(t, m.AddHost(host, WithChecker(chk)))

	// (true) 事件出现，且恰在 ≥2 次成功探测之后（rise=2）
	require.True(t, waitFor(t, func() bool { return len(col.snapshot()) >= 1 }, 5*time.Second),
		"first flip event should appear within 5s")
	events := col.snapshot()
	require.Len(t, events, 1)
	assert.Equal(t, host, events[0].host)
	assert.True(t, events[0].healthy, "first flip event should be (true)")
	assert.NotNil(t, events[0].result, "active flip events must carry a CheckResult")
	assert.GreaterOrEqual(t, chk.callCount(), 2, "(true) appears only after >= 2 successful probes (rise=2)")

	// 原子开关切换为恒 false → fall=2 → (false) 事件
	chk.setHealthy(false)
	require.True(t, waitFor(t, func() bool { return len(col.snapshot()) >= 2 }, 5*time.Second),
		"(false) flip event should appear within 5s after switching to false")
	events = col.snapshot()
	require.Len(t, events, 2)
	assert.Equal(t, host, events[1].host)
	assert.False(t, events[1].healthy, "second flip event should be (false)")
	assert.NotNil(t, events[1].result)
	assert.GreaterOrEqual(t, chk.callCount(), 4, "(false) appears only after >= 2 failed probes (fall=2, >= 4 calls cumulative)")

	// 已 DOWN 后续失败不再翻转：事件序列稳定为 [(true),(false)]
	assert.False(t, m.IsHealthy(host))
	assert.Equal(t, []cbEvent{events[0], events[1]}, col.snapshot(), "exactly 2 flip events overall (event-sequence level, SF5)")

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, m.Stop(stopCtx), "Stop(2s) should return normally")
}

// TestIntegration_C2_ColdStartFalse 是 M2-C2 集成侧：AddHost 后立即（Start 前）
// IsHealthy==false（default_down 安全启动）、GetHealthyHosts 为空。
// Start 前操作 → 用 fakeScheduler seam（确定性控制调度，与真实调度器解耦）。
func TestIntegration_C2_ColdStartFalse(t *testing.T) {
	fs := &fakeScheduler{}
	m := newTestManager(t, fs,
		WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()))

	const host = "c2-cold:80"
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{})))

	// 立即断言（未 Start、未探测）
	assert.False(t, m.IsHealthy(host), "cold-start IsHealthy must be false (C2)")
	assert.Empty(t, m.GetHealthyHosts(), "cold-start GetHealthyHosts must be empty (C2)")
}

// TestIntegration_C4_LexicographicAnyOrder 是 M2-C4 集成侧：5 组固定排列
// （禁用随机数，G-3 审计纪律）注册 {a,b,c,d,e}，全部经 probeFn 直调置健康
// （rise=1）→ 每组 GetHealthyHosts() 恒为字典序 [a,b,c,d,e]。
// 顺序敏感断言用 assert.Equal（禁 ElementsMatch，docs/08 §1.1）。
func TestIntegration_C4_LexicographicAnyOrder(t *testing.T) {
	// 5 组固定排列（含恒等、逆序与 3 组乱序），不依赖任何随机源
	permutations := [][]string{
		{"a", "b", "c", "d", "e"},
		{"e", "d", "c", "b", "a"},
		{"c", "a", "e", "b", "d"},
		{"b", "e", "d", "a", "c"},
		{"d", "c", "b", "e", "a"},
	}
	want := []string{"a", "b", "c", "d", "e"}

	for i, perm := range permutations {
		fs := &fakeScheduler{}
		m := newTestManager(t, fs,
			WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()), WithRise(1))
		for _, h := range perm {
			fchk := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
			require.NoError(t, m.AddHost(h, WithChecker(fchk)))
		}
		for _, h := range perm {
			m.probeFn(context.Background(), h) // rise=1：一次成功即翻转健康
			require.True(t, m.IsHealthy(h), "permutation %d: host %s should already be healthy", i, h)
		}
		assert.Equal(t, want, m.GetHealthyHosts(), "permutation %d %v: output must always be lexicographic (C4, map iteration order forbidden)", i, perm)
	}
}

// TestIntegration_C6_ANDTruthTableEndToEnd 是 M2-C6 集成侧：真实 TimeWindowDetector
// （G2 默认构造，fakeClock 驱动，maxFails=1/failTimeout=10s）+ probeFn 直调，
// AND 真值表 4 组合端到端：(1) 探测成功+无被动失败→true；(2) 探测成功+ReportFailure
// 翻转→false；(3) 探测失败+被动健康→false；(4) 双失败→false。
// 每步断言 IsHealthy 与最近回调 healthy 快照一致。
func TestIntegration_C6_ANDTruthTableEndToEnd(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	col := &cbCollector{}
	// 探测序列：第 1 次成功（(1)(2)用），第 2 次失败（(3)用）
	fchk := &fakeChecker{results: []fakeCheckResult{
		{result: &checker.CheckResult{Healthy: true}},
		{result: &checker.CheckResult{Healthy: false}},
	}}
	m := newTestManager(t, fs,
		WithClock(fc), WithRise(1), WithFall(1),
		WithPassiveMaxFails(1), WithPassiveFailTimeout(10*time.Second), // 真实 TimeWindowDetector（不注入 fake）
		WithOnStateChange(col.onChange))

	const host = "c6:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))

	// lastEventHealthy 返回最近回调的 healthy 快照（无回调时返回初值 false=冷启动）。
	lastEventHealthy := func() bool {
		events := col.snapshot()
		if len(events) == 0 {
			return false
		}
		return events[len(events)-1].healthy
	}

	// (1) active=true（探测成功 rise=1）∧ passive=true（无被动失败）→ true
	m.probeFn(context.Background(), host)
	assert.True(t, m.IsHealthy(host), "(1) active success + passive healthy -> true")
	events := col.snapshot()
	require.Len(t, events, 1, "(1) should trigger exactly 1 flip callback")
	assert.True(t, events[0].healthy)
	assert.NotNil(t, events[0].result, "active flip carries a CheckResult")
	assert.Equal(t, m.IsHealthy(host), lastEventHealthy(), "(1) IsHealthy matches the callback snapshot")

	// (2) active=true ∧ passive=false（maxFails=1：一次 ReportFailure 即翻转）→ false
	m.ReportFailure(host)
	assert.False(t, m.IsHealthy(host), "(2) passive flip -> false")
	events = col.snapshot()
	require.Len(t, events, 2, "(2) should trigger the passive flip callback")
	assert.False(t, events[1].healthy)
	assert.Nil(t, events[1].result, "passive flip callback result must be nil (R-d)")
	assert.Equal(t, m.IsHealthy(host), lastEventHealthy(), "(2) IsHealthy matches the callback snapshot")

	// (3) active=false（探测失败 fall=1）∧ passive=true（窗口到期后 ReportSuccess 惰性恢复）→ false
	fc.Advance(11 * time.Second) // 超过 failTimeout=10s
	m.ReportSuccess(host)        // 惰性恢复 → passive=true；active 仍 true → 综合翻转 true
	assert.True(t, m.IsHealthy(host), "(3) precondition: combined should be true after passive recovery")
	events = col.snapshot()
	require.Len(t, events, 3)
	assert.True(t, events[2].healthy)
	assert.Nil(t, events[2].result, "passive recovery flip result==nil (R-d)")
	assert.Equal(t, m.IsHealthy(host), lastEventHealthy(), "(3) passive-recovery step: IsHealthy matches the callback snapshot")

	m.probeFn(context.Background(), host) // 第 2 次探测失败 → active=false（fall=1）
	assert.False(t, m.IsHealthy(host), "(3) probe failure + passive healthy -> false")
	events = col.snapshot()
	require.Len(t, events, 4)
	assert.False(t, events[3].healthy)
	assert.NotNil(t, events[3].result, "active flip carries a CheckResult")
	assert.Equal(t, m.IsHealthy(host), lastEventHealthy(), "(3) IsHealthy matches the callback snapshot")

	// (4) active=false ∧ passive=false（再次 ReportFailure 翻转被动镜像）→ false
	m.ReportFailure(host)
	assert.False(t, m.IsHealthy(host), "(4) both failed -> false")
	events = col.snapshot()
	assert.Len(t, events, 4, "(4) combined value unchanged (false -> false) -> no new callback triggered")
	assert.Equal(t, m.IsHealthy(host), lastEventHealthy(), "(4) IsHealthy matches the callback snapshot")
}

// TestIntegration_C7_PanicIsolationDualSource 是 M2-C7 集成侧（双源 panic）：
// 真实 Scheduler；host-A 注入 panic Checker（recover 点 (1)），host-B 注入 panic
// 回调+sink（recover 点 (2)）。断言：A 的 LastResult.Err 含 "checker panic:" 且
// A 持续被探测（callCount 递增）；sink 恰收 B 的 (host,err)；workers 存活
// （A/B 均持续有探测）；Stop(2s) 正常返回不挂死。
func TestIntegration_C7_PanicIsolationDualSource(t *testing.T) {
	fc := newFakeClock()
	sink := &sinkCollector{}
	chkA := &fakeChecker{results: []fakeCheckResult{{doPanic: true}}}                               // A：每轮 panic
	chkB := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}} // B：成功 → rise=1 翻转 → 回调 panic
	m := newTestManager(t, nil,                                                                     // 真实 Scheduler
		WithInterval(20*time.Millisecond), WithClock(fc), WithRise(1), WithFall(1),
		WithPassiveDetector(newFakePassiveDetector()),
		WithOnStateChange(panicStateChange), WithOnInternalError(sink.onInternalError))

	require.NoError(t, m.Start(context.Background()))
	const hostA, hostB = "c7-a:80", "c7-b:80"
	require.NoError(t, m.AddHost(hostA, WithChecker(chkA)))
	require.NoError(t, m.AddHost(hostB, WithChecker(chkB)))

	// sink 恰收 B 的回调 panic（B 翻转一次 → panicStateChange → recover (2) → sink）
	require.True(t, waitFor(t, func() bool { return len(sink.snapshot()) >= 1 }, 5*time.Second),
		"sink should receive B's callback-panic notification within 5s")
	sinkEvents := sink.snapshot()
	require.Len(t, sinkEvents, 1, "sink receives exactly 1 (B flips only once; A never flips so triggers no callback)")
	assert.Equal(t, hostB, sinkEvents[0].host)
	assert.ErrorContains(t, sinkEvents[0].err, "StateChangeCallback panic for host")

	// A 的 panic 被 safeCheck 隔离：LastResult.Err 含 "checker panic:"
	require.True(t, waitFor(t, func() bool {
		lr := m.GetLastResult(hostA)
		return lr != nil && lr.Err != nil
	}, 5*time.Second), "A should have a probe result within 5s")
	lrA := m.GetLastResult(hostA)
	require.NotNil(t, lrA)
	assert.False(t, lrA.Healthy)
	assert.ErrorContains(t, lrA.Err, "checker panic:", "A's LastResult.Err should contain the fixed prefix (recover (1))")

	// workers 存活：A 持续被探测且 B 也持续被探测（callCount 均递增）
	callsA1, callsB1 := chkA.calls(), chkB.calls()
	require.True(t, waitFor(t, func() bool {
		return chkA.calls() > callsA1+1 && chkB.calls() > callsB1+1
	}, 5*time.Second), "workers survive: both A and B should keep being probed (callCounts increase)")
	assert.True(t, m.IsHealthy(hostB), "B stays healthy after its flip (callback panic does not affect the state machine)")

	// sink 不因后续探测增加（B 不再翻转）
	assert.Len(t, sink.snapshot(), 1, "sink holds exactly 1 entry throughout")

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, m.Stop(stopCtx), "Stop(2s) returns normally without hanging (worker pool healthy after dual-source panic isolation)")
}

// TestIntegration_C8_NoLeakQueryableAfterStop 是 M2-C8 集成侧：
// baseline→New+Start+2 host 探测数轮→Stop(2s)；2s 内轮询 NumGoroutine 回落至
// baseline（±0，workers+timer 协程全部退出）；Stop 后查询 API 仍返回有效值
// （优雅降级，hosts 状态不清空）。
func TestIntegration_C8_NoLeakQueryableAfterStop(t *testing.T) {
	baseline := runtime.NumGoroutine() // 必须在 New 之前（New 构造期启动 timer 搬运协程）

	fc := newFakeClock()
	m := newTestManager(t, nil, // 真实 Scheduler
		WithInterval(20*time.Millisecond), WithClock(fc), WithRise(1),
		WithPassiveDetector(newFakePassiveDetector()))
	require.NoError(t, m.Start(context.Background()))

	const h1, h2 = "c8-h1:80", "c8-h2:80"
	chk1 := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
	chk2 := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
	require.NoError(t, m.AddHost(h1, WithChecker(chk1)))
	require.NoError(t, m.AddHost(h2, WithChecker(chk2)))

	// 探测数轮：双 host 健康且各 ≥3 次探测
	require.True(t, waitFor(t, func() bool {
		return m.IsHealthy(h1) && m.IsHealthy(h2) && chk1.calls() >= 3 && chk2.calls() >= 3
	}, 5*time.Second), "precondition: both hosts should be probed for several rounds and flip to healthy")

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, m.Stop(stopCtx), "Stop(2s) should return successfully")

	// 2s 内轮询 goroutine 计数回落至 baseline（±0）
	current := runtime.NumGoroutine()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && current > baseline {
		time.Sleep(20 * time.Millisecond)
		current = runtime.NumGoroutine()
	}
	assert.LessOrEqual(t, current, baseline, "no goroutine leak after Stop (falls back to baseline +/-0, C8)")

	// Stop 后仍可查询（优雅降级）
	assert.True(t, m.IsHealthy(h1))
	assert.True(t, m.IsHealthy(h2))
	assert.Equal(t, []string{h1, h2}, m.GetHealthyHosts())
	assert.NotNil(t, m.GetLastResult(h1))
	assert.NotNil(t, m.GetLastResult(h2))
	assert.Equal(t, 2, m.Hosts())
}

// TestIntegration_StopBeforeStart_NoGoroutineLeak 是 ERC P2-2 修复的集成侧证明：
// New() 构造期即启动 workqueue TimerQueue 的 scheduler 搬运 goroutine，旧实现
// "未 Start 的 Stop 为完全 no-op"使该 goroutine 在 New→(未 Start)→Stop 或
// New→弃用 生命周期下永不回收（与 docs/02 Stop 无泄漏保证冲突）。
// baseline→New（不 Start）→Stop(2s) 返回 nil→2s 内轮询 NumGoroutine 回落至
// baseline（±0，构造期协程经 Shutdown 回收）；Stop 后 Start 返回 ErrStopped
// （生命周期一次性）。真实调度器未 Run 时 Shutdown=立即关闭+有界等待即刻返回（R3）。
func TestIntegration_StopBeforeStart_NoGoroutineLeak(t *testing.T) {
	baseline := runtime.NumGoroutine() // 必须在 New 之前（New 构造期启动 timer 搬运协程）

	m := newTestManager(t, nil, // 真实 Scheduler；不 Start，Stop 即构造期协程的唯一回收点
		WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()))

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, m.Stop(stopCtx), "Stop without Start must succeed and shut down the construction-time scheduler goroutine (ERC P2-2)")

	// 2s 内轮询 goroutine 计数回落至 baseline（±0）
	current := runtime.NumGoroutine()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && current > baseline {
		time.Sleep(20 * time.Millisecond)
		current = runtime.NumGoroutine()
	}
	assert.LessOrEqual(t, current, baseline,
		"the construction-time scheduler goroutine must be reclaimed by Stop even without Start (no leak, ERC P2-2)")

	// 生命周期一次性：Stop 后 Start fail-fast（与 AddHost after Stop 对齐）
	assert.ErrorIs(t, m.Start(context.Background()), ErrStopped, "Start after Stop must fail fast (one-shot lifecycle)")
}

// TestIntegration_C9_SingleWriterNoOverlap 是 M2-C9 集成侧：真实 TimerQueue
// （幂等模式），interval=10ms、workers=4、单 host；Check 内 enter/exit 计数
// 严格配对，并发 >1 即 violation；运行 ~500ms 后断言 violations==0 且总探测 ≥10
// （进入/退出序号严格配对的集成侧等价断言）。
func TestIntegration_C9_SingleWriterNoOverlap(t *testing.T) {
	fc := newFakeClock()
	chk := &overlapDetectorChecker{}
	m := newTestManager(t, nil, // 真实 TimerQueue
		WithInterval(10*time.Millisecond), WithWorkers(4),
		WithSpreadSlots(8), // 机械调和：R-b 要求 interval/spreadSlots ≥ 1ms（10ms/16 < 1ms 非法），8 槽满足
		WithClock(fc), WithRise(1),
		WithPassiveDetector(newFakePassiveDetector()))
	require.NoError(t, m.Start(context.Background()))
	require.NoError(t, m.AddHost("c9-single:80", WithChecker(chk)))

	time.Sleep(500 * time.Millisecond) // 运行 ~500ms（interval=10ms+探测 2ms → 期望数十次探测）

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, m.Stop(stopCtx))

	assert.Equal(t, int32(0), chk.violations.Load(), "single writer, no overlap: at most 1 in-flight probe per host at any moment (C9)")
	assert.GreaterOrEqual(t, chk.calls.Load(), int32(10), "total probe count within ~500ms should be >= 10")
	assert.Equal(t, int32(0), chk.active.Load(), "no in-flight probes after teardown (enter/exit strictly paired)")
}

// TestIntegration_ReportRemoveHostRace_Stress 是 ERC P2-A 守卫的竞态压测
// （概率性补充，不替代 TestReport_ReRegisterInFlight_StaleReportDropped 的
// 确定性 oracle）：多轮循环 AddHost → 并发（4 个上报 goroutine 交替
// ReportFailure/ReportSuccess × 主动 probeFn × RemoveHost+AddHost 同名重注册），
// fakeClock 每次上报前推进 2min 使被动窗口持续过期 → 判死/恢复反复翻转，
// 最大化命中"查找→cbMu"守卫窗口与墓碑/重注册交错。
// 断言：-race 零报告（门禁执行侧保证）、无死锁/panic（限时内完成）、
// 静默后终态一致（最终 RemoveHost 后 Hosts()==0、IsHealthy==false、
// 回调计数不再增长——无迟到幽灵事件）。
func TestIntegration_ReportRemoveHostRace_Stress(t *testing.T) {
	fc := newFakeClock()
	col := &cbCollector{}
	det := passive.NewTimeWindowDetector(1, time.Minute, fc) // maxFails=1：窗口过期时每次失败/成功交替即翻转
	m := newTestManager(t, &fakeScheduler{},
		WithClock(fc), WithPassiveDetector(det), WithRise(1), WithFall(1),
		WithOnStateChange(col.onChange))

	const host = "stress-race:80"
	const rounds = 20
	healthy := &checker.CheckResult{Healthy: true}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for r := 0; r < rounds; r++ {
			if !assert.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{results: []fakeCheckResult{{result: healthy}}}))) {
				return // 前置失败：终止压测主体（非测试 goroutine 只用 assert）
			}
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 10; j++ {
						fc.Advance(2 * time.Minute) // 窗口过期 → 失败判死可翻转
						m.ReportFailure(host)
						fc.Advance(2 * time.Minute) // 窗口过期 → 成功惰性恢复可翻转
						m.ReportSuccess(host)
					}
				}()
			}
			wg.Add(1)
			go func() { defer wg.Done(); m.probeFn(context.Background(), host) }()
			wg.Add(1)
			go func() {
				defer wg.Done()
				assert.NoError(t, m.RemoveHost(host))                                                                        // 与在途上报/探测竞态（墓碑窗口）
				assert.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{results: []fakeCheckResult{{result: healthy}}}))) // 同名重注册（注册代窗口）
			}()
			wg.Wait()
			if !assert.NoError(t, m.RemoveHost(host)) { // 静默收尾：下一轮前确保无 host
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("stress rounds did not finish within 30s (suspected deadlock in the report x RemoveHost interleave)")
	}

	// 终态一致性
	assert.Equal(t, 0, m.Hosts(), "no host left after the final RemoveHost")
	assert.False(t, m.IsHealthy(host), "a removed host must not report healthy")
	n1 := len(col.snapshot())
	time.Sleep(50 * time.Millisecond) // 静默观察期：全部上报/探测 goroutine 已经 wg.Wait 收口
	assert.Len(t, col.snapshot(), n1, "no delayed (ghost) callbacks after quiescence")
}

// TestIntegration_ReportRemoveHostRace_NoDetectorLeak_Stress 是 P1-3 修复
// （report identity 失配路径主动 Reset 回收孤儿条目）的竞态压测（概率性补充，
// 不替代 TestReport_RemoveHostRace_OrphanEntryReclaimed 的确定性 oracle）：
// 多轮循环 AddHost → 并发（4 个上报 goroutine 交替 ReportFailure/ReportSuccess
// × 主动 probeFn × RemoveHost+AddHost 同名重注册），fakeClock 每次上报前推进
// 2min 使被动窗口持续过期，maxFails=1 使任何复活条目单次失败即翻转为
// passiveHealthy=false——最大化命中"lookup→Record 窗口内 Reset 落地"的复活交错。
//
// 泄漏判别（经导出行为观测，detector 内部条目数不可从根包读取）：全部轮次
// 静默收尾后，同名全新注册的被动失败上报必须能正常翻转——若竞态轮次泄漏了
// 孤儿条目（passiveHealthy=false，失败路径永不翻转），判别上报会被静默吞掉
// （IsHealthy 恒 true、无翻转回调）。断言：-race 零报告（门禁执行侧保证）、
// 限时内完成（无死锁/panic）、终局判别翻转正常（无泄漏）。
func TestIntegration_ReportRemoveHostRace_NoDetectorLeak_Stress(t *testing.T) {
	fc := newFakeClock()
	col := &cbCollector{}
	det := passive.NewTimeWindowDetector(1, time.Minute, fc) // maxFails=1：复活条目单次失败即翻转
	m := newTestManager(t, &fakeScheduler{},
		WithClock(fc), WithPassiveDetector(det), WithRise(1), WithFall(1),
		WithOnStateChange(col.onChange))

	const host = "leak-stress:80"
	const rounds = 15
	healthy := &checker.CheckResult{Healthy: true}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for r := 0; r < rounds; r++ {
			if !assert.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{results: []fakeCheckResult{{result: healthy}}}))) {
				return // 前置失败：终止压测主体（非测试 goroutine 只用 assert）
			}
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 8; j++ {
						fc.Advance(2 * time.Minute) // 窗口过期 → 失败判死可翻转
						m.ReportFailure(host)
						fc.Advance(2 * time.Minute) // 窗口过期 → 成功惰性恢复可翻转
						m.ReportSuccess(host)
					}
				}()
			}
			wg.Add(1)
			go func() { defer wg.Done(); m.probeFn(context.Background(), host) }()
			wg.Add(1)
			go func() {
				defer wg.Done()
				assert.NoError(t, m.RemoveHost(host))                                                                        // 与在途上报竞态（lookup→Record 窗口）
				assert.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{results: []fakeCheckResult{{result: healthy}}}))) // 同名重注册（identity 失配第二分支）
			}()
			wg.Wait()
			if !assert.NoError(t, m.RemoveHost(host)) { // 静默收尾：下一轮前确保无 host
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("leak stress rounds did not finish within 60s (suspected deadlock in the report x RemoveHost x Reset interleave)")
	}

	// 泄漏判别：全新注册后被动失败翻转链路必须正常（无孤儿条目残留）
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{results: []fakeCheckResult{{result: healthy}}})))
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host), "precondition: the fresh registration is probed UP")
	before := len(col.snapshot())
	m.ReportFailure(host)
	assert.False(t, m.IsHealthy(host),
		"no detector entry leak across the race rounds: a fresh registration's failure report must flip passively (a leaked orphan entry stuck at passiveHealthy=false would silently swallow it and leave IsHealthy true)")
	assert.Len(t, col.snapshot(), before+1, "the discriminating passive flip delivers exactly 1 callback")

	// 终态收尾
	require.NoError(t, m.RemoveHost(host))
	assert.Equal(t, 0, m.Hosts(), "no host left after the final RemoveHost")
}
