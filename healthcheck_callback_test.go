package healthcheck

// ST-08 回调/错误传播测试：docs/08 §3.5 错误传播层 + §3.6 确定性交错判别力
// oracle（DoD #10/#11 主责）。复用 healthcheck_basic_test.go 的 fake 辅助与
// waitFor（healthcheck_integration_test.go），不重复定义；本文件仅新增
// oracle/不变式专用的局部 Checker 替身（阻塞屏障、奇偶交替——basic 替身不具备）。
//
// 映射登记（docs/08 §3.5 行 9-10，任务卡裁定"不设新测试"）：
//   - 行 9（库自身 panic → 向上传播不被 recover，worker 端到端版）：worker
//     goroutine 内未 recover 的 panic 会崩整个测试进程，进程内不可断言。
//     以 ST-07 TestProbeFn_LibraryPanic_FailLoud（同包直调版）+ 审查记录替代。
//   - 行 10（defer 收尾保证 Done/PutAfter 在 panic unwind 时仍执行，
//     TestDeferTail_DoneRearmAfterPanic）：测试本体=ST-07
//     TestProbeFn_LibraryPanic_FailLoud（已断言 panic unwind 期间 Done+Rearm
//     完成）；集成侧不可测（同上理由），映射表登记替代关系。
//
// 断言纪律（docs/08 §1.1）：非测试 goroutine 内只用 assert 禁 require；
// t.Fatal 类致命控制流（select 超时防死锁）限测试 goroutine。

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
)

// ———————————————————————————— 局部 Checker 替身 ————————————————————————————

// oracleChecker 是 §3.6 判别力 oracle 专用替身：第 1 次 Check 返回 Healthy=true；
// 第 2 次 Check 阻塞在 <-gateW（屏障 1），放行后 close(checkDone)（屏障 2，
// sync.Once）并返回 Healthy=false——把交错精确钉死在"W 已出 safeCheck、正取 cbMu"点。
type oracleChecker struct {
	calls     atomic.Int32
	gateW     chan struct{} // 测试侧注入：G 回调内 close 放行
	checkDone chan struct{} // Check 已返回信号（close 一次）
	once      sync.Once
}

// 编译期断言：满足 Checker 接口。
var _ checker.Checker = (*oracleChecker)(nil)

// Check 第 1 次成功；第 2 次经双屏障后返回失败。
func (c *oracleChecker) Check(ctx context.Context) (*checker.CheckResult, error) {
	if c.calls.Add(1) == 1 {
		return &checker.CheckResult{Healthy: true}, nil
	}
	<-c.gateW // 屏障 1：等待 G 进入回调#3（G 已持 cbMu）
	c.once.Do(func() { close(c.checkDone) })
	return &checker.CheckResult{Healthy: false}, nil // 屏障 2 已发出：W 正尝试取 cbMu
}

// alternatingChecker 是不变式并发测试替身：按调用奇偶交替 true/false
// （奇数次→true，偶数次→false），配合 rise=fall=1 使每轮探测都翻转主动状态。
type alternatingChecker struct {
	calls atomic.Int32
}

// 编译期断言：满足 Checker 接口。
var _ checker.Checker = (*alternatingChecker)(nil)

// Check 返回奇偶交替结果。
func (c *alternatingChecker) Check(ctx context.Context) (*checker.CheckResult, error) {
	return &checker.CheckResult{Healthy: c.calls.Add(1)%2 == 1}, nil
}

// ———————————————————————————— §3.6 判别力 oracle（DoD #10） ————————————————————————————

// TestCallbackOrdering_DeterministicInterleave 是门禁 #10 的判别力核心
// （docs/08 §3.6 五步骤+双屏障逐字落实，禁止简化为"仅阻塞 Checker"——
// 那对旧设计无判别力，会产生虚假绿灯）。
//
// 判别力原理：G（宿主请求 goroutine，被动恢复翻转）持 cbMu 进入回调#3，回调内
// 放行 W（probe-worker，主动探测失败 fall=1）的 Check 并等待"Check 已返回"信号
// （确保 W 已出 safeCheck、正尝试取 cbMu），随后白名单重读 IsHealthy(H)：
//   - 新设计（cbMu 在状态变更之前获取）：W 阻塞在 cbMu，状态未变 → true → PASS
//   - 旧设计（状态变更先于 cbMu）：W 已 ApplyResult 翻转 → false → FAIL
//
// 前置：H 达 Active=true（回调#1）、Passive=false（maxFails=1 立即判死，回调#2）；
// fakeClock 推进 11s 超过 failTimeout（10s）→ ReportSuccess 触发惰性恢复翻转。
// 交错确定性：W 的 Check 阻塞至 G 进入回调#3（G 已持 cbMu）才放行，故 G 必先于
// W 持有 cbMu——构造上排除竞态窗口，-race -count=100 稳定。
func TestCallbackOrdering_DeterministicInterleave(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	gateW := make(chan struct{})
	checkDone := make(chan struct{})
	oracleVal := make(chan bool, 1)

	var cbCalls atomic.Int32
	var m *HealthManager // 闭包前置声明：testCb 内白名单调用 m.IsHealthy
	// testCb 分派（原子调用计数）：第 3 次调用且特征 result==nil && healthy==true
	// （即 G 的被动恢复翻转）内执行 oracle 三步：放行 W → 等 Check 返回 → 重读。
	testCb := func(host string, healthy bool, result *CheckResult) {
		if n := cbCalls.Add(1); n == 3 && result == nil && healthy {
			close(gateW)                   // 步骤(2)：放行 W 的 Check（G 持 cbMu）
			<-checkDone                    // 步骤(3)：等待"Check 已返回"（W 已出 safeCheck 正尝试取 cbMu）
			oracleVal <- m.IsHealthy(host) // 步骤(4)：白名单合法调用（docs/02）
		}
	}
	m = newTestManager(t, fs, // seam 替换 fakeScheduler，不起 worker；detector 用 New 默认真实 TimeWindowDetector（fc 驱动）
		WithRise(1), WithFall(1), WithClock(fc),
		WithPassiveMaxFails(1), WithPassiveFailTimeout(10*time.Second),
		WithOnStateChange(testCb))

	const host = "H"
	chk := &oracleChecker{gateW: gateW, checkDone: checkDone}
	require.NoError(t, m.AddHost(host, WithChecker(chk)))

	ctx := context.Background()
	// 前置(1)：第 1 次探测成功 → Active=true（回调#1: true+result）
	m.probeFn(ctx, host)
	require.True(t, m.IsHealthy(host), "precondition (1): Active should be true")
	// 前置(2)：ReportFailure（maxFails=1 → 立即翻转）→ Passive=false（回调#2: false+nil）；综合=false
	m.ReportFailure(host)
	require.False(t, m.IsHealthy(host), "precondition (2): combined health should be false")
	require.Equal(t, int32(2), cbCalls.Load(), "preconditions should produce exactly 2 callbacks (prerequisite for the n==3 signature match)")
	// 前置(3)：推进超过 failTimeout（10s）→ 下一次 ReportSuccess 触发惰性恢复
	fc.Advance(11 * time.Second)

	// 执行：G=ReportSuccess（惰性恢复→翻转→回调#3 持 cbMu）；W=probeFn（Check 阻塞→放行后失败→取 cbMu 被 G 阻塞）
	var gw sync.WaitGroup
	gw.Add(2)
	go func() { defer gw.Done(); m.ReportSuccess(host) }() // G：宿主请求 goroutine
	go func() { defer gw.Done(); m.probeFn(ctx, host) }()  // W：probe-worker goroutine

	// oracle 断言（测试 goroutine；t.Fatal 防死锁属致命控制流，docs/08 §1.1 允许）
	select {
	case v := <-oracleVal:
		assert.True(t, v, "oracle discrimination assertion: re-reading IsHealthy(H) inside the callback must be true (new design: W blocked on cbMu, state unchanged; old design: already flipped to false -> FAIL)")
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: oracleVal not received within 5s (the double-barrier interleave did not happen as expected)")
	}

	// 收尾：等 G/W 完成（回调#4 false+result 可达、fakeScheduler done 日志），断言无泄漏
	gw.Wait()
	assert.Equal(t, int32(4), cbCalls.Load(), "exactly 4 callbacks overall (2 preconditions + G recovery flip + W failure flip)")
	assert.False(t, m.IsHealthy(host), "final state: W's failure result applied (fall=1 -> Active=false)")
	assert.Len(t, fs.callsOf("done"), 2, "precondition probe + W probe each complete teardown once (no processing leak)")
	assert.Len(t, fs.callsOf("rearm"), 2, "both probes were rearmed (probing never stops)")
}

// ———————————————————————————— §3.5 错误传播层 ————————————————————————————

// TestErrorPropagation_CheckerPanic_EndToEnd（docs/08 §3.5 行 4）：真实 Scheduler
// 端到端，panic Checker → safeCheck recover (1) 合成 LastResult.Err 含
// "checker panic:"；worker 存活；该 host 下一轮继续被探测（callCount ≥2，2s 内）。
func TestErrorPropagation_CheckerPanic_EndToEnd(t *testing.T) {
	fc := newFakeClock()
	fchk := &fakeChecker{results: []fakeCheckResult{{doPanic: true}}}
	m := newTestManager(t, nil, // 真实 Scheduler
		WithInterval(20*time.Millisecond), WithClock(fc),
		WithPassiveDetector(newFakePassiveDetector()))
	require.NoError(t, m.Start(context.Background()))

	const host = "panic-e2e:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))

	// 2s 内：LastResult.Err 含 "checker panic:" 且 callCount ≥2（下一轮继续被探测）
	require.True(t, waitFor(t, func() bool {
		lr := m.GetLastResult(host)
		return lr != nil && lr.Err != nil && fchk.calls() >= 2
	}, 2*time.Second), "panic result should be synthesized within 2s and the host should keep being probed (callCount >= 2)")
	lr := m.GetLastResult(host)
	require.NotNil(t, lr)
	assert.False(t, lr.Healthy)
	assert.ErrorContains(t, lr.Err, "checker panic:")
	assert.ErrorContains(t, lr.Err, "injected Check panic", "synthesized Err should carry the panic value")

	// worker 存活：callCount 持续递增
	c1 := fchk.calls()
	require.True(t, waitFor(t, func() bool { return fchk.calls() > c1+1 }, 2*time.Second),
		"worker survives: subsequent rounds keep probing")

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, m.Stop(stopCtx), "Stop(2s) completes normally")
}

// TestErrorPropagation_CallbackPanic_Sink（docs/08 §3.5 行 5）：真实 Scheduler，
// panic 回调+sink → sink 恰收 1 次 (host, err 含 "StateChangeCallback panic for host")；
// worker 池存活（其余 host 持续探测）；Stop(2s) 正常完成。
func TestErrorPropagation_CallbackPanic_Sink(t *testing.T) {
	fc := newFakeClock()
	sink := &sinkCollector{}
	chkFlip := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}   // 翻转 → 回调 panic
	chkProbe := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: false}}}} // 永不翻转（冷启动 false）→ 只作存活探针
	m := newTestManager(t, nil,                                                                          // 真实 Scheduler
		WithInterval(20*time.Millisecond), WithClock(fc), WithRise(1), WithFall(1),
		WithPassiveDetector(newFakePassiveDetector()),
		WithOnStateChange(panicStateChange), WithOnInternalError(sink.onInternalError))
	require.NoError(t, m.Start(context.Background()))

	const flipHost, probeHost = "cb-panic-sink:80", "cb-probe-alive:80"
	require.NoError(t, m.AddHost(flipHost, WithChecker(chkFlip)))
	require.NoError(t, m.AddHost(probeHost, WithChecker(chkProbe)))

	// sink 恰收 1 次（flipHost 翻转一次 → recover (2) → sink）
	require.True(t, waitFor(t, func() bool { return len(sink.snapshot()) >= 1 }, 2*time.Second),
		"sink should receive the callback-panic notification within 2s")
	events := sink.snapshot()
	require.Len(t, events, 1, "sink receives exactly 1 (only one flip)")
	assert.Equal(t, flipHost, events[0].host)
	assert.ErrorContains(t, events[0].err, "StateChangeCallback panic for host")
	assert.ErrorContains(t, events[0].err, "callback boom", "err should carry the panic value")

	// worker 池存活：其余 host（probeHost）持续探测，且 sink 不再增加
	p1 := chkProbe.calls()
	require.True(t, waitFor(t, func() bool { return chkProbe.calls() > p1+2 }, 2*time.Second),
		"worker pool survives: remaining hosts keep being probed")
	assert.Len(t, sink.snapshot(), 1, "sink holds exactly 1 entry throughout (flipHost never flips again)")

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, m.Stop(stopCtx), "Stop(2s) completes normally without hanging")
}

// TestErrorPropagation_CallbackPanic_NilSink（docs/08 §3.5 行 6）：panic 回调、
// 无 sink → panic 被隔离并丢弃（文档化声明 docs/06 §1.3）；host 继续被探测；
// Stop 正常。
func TestErrorPropagation_CallbackPanic_NilSink(t *testing.T) {
	fc := newFakeClock()
	fchk := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
	m := newTestManager(t, nil, // 真实 Scheduler
		WithInterval(20*time.Millisecond), WithClock(fc), WithRise(1), WithFall(1),
		WithPassiveDetector(newFakePassiveDetector()),
		WithOnStateChange(panicStateChange)) // 不注入 sink（nil）
	require.NoError(t, m.Start(context.Background()))

	const host = "cb-panic-nilsink:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))

	// 翻转发生且 panic 被隔离丢弃：IsHealthy 变 true、进程不崩、探测持续
	require.True(t, waitFor(t, func() bool { return m.IsHealthy(host) }, 2*time.Second),
		"should flip to healthy within 2s (callback panic isolated, state machine unaffected)")
	p1 := fchk.calls()
	require.True(t, waitFor(t, func() bool { return fchk.calls() > p1+1 }, 2*time.Second),
		"host keeps being probed (worker survives)")

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, m.Stop(stopCtx), "Stop(2s) is normal")
}

// TestErrorPropagation_DifferentHosts_Concurrent（docs/08 §3.5 行 7）：
// 3 个 host 同时翻转（并发直调 probeFn），各回调内经"到达计数+close 屏障"会合
// （等价 wg.Done+wg.Wait，附 2s 超时：若回调被全局串行化则超时 FAIL 而非挂死）
// → 证明不同 host 回调可并发；-race 零报告（门禁执行侧保证）。
func TestErrorPropagation_DifferentHosts_Concurrent(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	const n = 3
	hosts := []string{"conc-1:80", "conc-2:80", "conc-3:80"}

	var arrived atomic.Int32
	var timedOut atomic.Int32
	barrier := make(chan struct{})
	var barrierOnce sync.Once
	cb := func(host string, healthy bool, result *CheckResult) {
		if arrived.Add(1) == n {
			barrierOnce.Do(func() { close(barrier) }) // 第 3 个到达 → 全体放行（并发会合成功）
		}
		select {
		case <-barrier:
		case <-time.After(2 * time.Second): // 被串行化 → 超时 FAIL（不挂死测试进程）
			timedOut.Add(1)
		}
	}
	m := newTestManager(t, fs,
		WithClock(fc), WithRise(1), WithFall(1),
		WithPassiveDetector(newFakePassiveDetector()), WithOnStateChange(cb))
	for _, h := range hosts {
		fchk := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
		require.NoError(t, m.AddHost(h, WithChecker(fchk)))
	}

	// 3 个 host 同时翻转：并发直调 probeFn（回调在各 goroutine 内会合）
	var wg sync.WaitGroup
	for _, h := range hosts {
		wg.Add(1)
		go func(h string) { defer wg.Done(); m.probeFn(context.Background(), h) }(h)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent probeFn calls did not finish within 10s (callbacks suspected deadlock)")
	}

	assert.Equal(t, int32(n), arrived.Load(), "flip callbacks of all 3 hosts should arrive")
	assert.Equal(t, int32(0), timedOut.Load(),
		"zero barrier timeouts: callbacks of different hosts can run concurrently (global-lock serialization would necessarily time out)")
}

// ———————————————————————————— §3.5 不变式随机并发 ————————————————————————————

// TestInvariant_NotifiedConsistency_UnderConcurrency（docs/08 §3.5 行 8）：
// K=50 轮三路并发钉死全局不变式「任何时刻 lastNotifiedHealthy == IsHealthy」：
//   - goroutine-A：循环直调 probeFn（alternatingChecker 奇偶交替 true/false，
//     rise=fall=1 → 每轮翻转主动状态）
//   - goroutine-B：循环 ReportFailure→fc.Advance(11s)→ReportSuccess
//     （maxFails=1 → 被动翻转往复）
//   - goroutine-C（观察者）：循环在 mh.cbMu 临界区内读 lastNotified 与
//     IsHealthy(H) 并断言相等（lastNotified 由回调在 cbMu 内写入，初值
//     false=冷启动；锁序 cbMu → manager.mu → HostState.mu 合法方向）
//
// -race -count=100 下零报告零失败（概率性补充，不替代 §3.6 判别力 oracle）。
func TestInvariant_NotifiedConsistency_UnderConcurrency(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}

	// lastNotified 受 host "H" 的 cbMu 保护：回调（notifyLocked 调用者持 cbMu）写、
	// 观察者 C 在同一 cbMu 临界区内读——初值 false=冷启动一致（C2）。
	var lastNotified bool
	testCb := func(host string, healthy bool, result *CheckResult) {
		lastNotified = healthy
	}
	m := newTestManager(t, fs,
		WithClock(fc), WithRise(1), WithFall(1),
		WithPassiveMaxFails(1), WithPassiveFailTimeout(10*time.Second), // 真实 TimeWindowDetector（fc 驱动）
		WithOnStateChange(testCb))

	const host = "H"
	require.NoError(t, m.AddHost(host, WithChecker(&alternatingChecker{})))
	mh, ok := m.getManagedHost(host)
	require.True(t, ok)

	const rounds = 50 // K=50
	var abwg sync.WaitGroup
	abwg.Add(2)
	// A：主动探测（每轮翻转）
	go func() {
		defer abwg.Done()
		for i := 0; i < rounds; i++ {
			m.probeFn(context.Background(), host)
		}
	}()
	// B：被动翻转往复（判死 → 窗口到期 → 惰性恢复）
	go func() {
		defer abwg.Done()
		for i := 0; i < rounds; i++ {
			m.ReportFailure(host)
			fc.Advance(11 * time.Second)
			m.ReportSuccess(host)
		}
	}()
	// C：观察者（cbMu 临界区内断言不变式；非测试 goroutine 只用 assert）
	var stop atomic.Bool
	cDone := make(chan struct{})
	go func() {
		defer close(cDone)
		for !stop.Load() {
			mh.cbMu.Lock()
			a := lastNotified
			b := m.IsHealthy(host)
			mh.cbMu.Unlock()
			assert.Equal(t, b, a, "invariant: lastNotifiedHealthy == IsHealthy at all times (inside the cbMu critical section)")
			runtime.Gosched()
		}
	}()

	abwg.Wait() // 等 A/B 完成（其间 C 持续观察）
	stop.Store(true)
	select {
	case <-cDone:
	case <-time.After(5 * time.Second):
		t.Fatal("observer C did not exit within 5s")
	}

	// 终态一致性（静止点复查）与无泄漏（A/B/C 均已 join）
	mh.cbMu.Lock()
	assert.Equal(t, m.IsHealthy(host), lastNotified, "final-state invariant holds")
	mh.cbMu.Unlock()
}
