package healthcheck

// ST-07 单 API 行为测试 + 测试替身（fakeClock/fakeChecker/fakeScheduler/fakePassiveDetector）。
// 本文件的 fake 辅助是 ST-08 的复用契约：ST-08 直接复用、不得重复定义（runbook ST-07 downstream_hint）。
// 并发/回调时序/确定性/泄漏等集成门禁归 ST-08；此处只测"单 API 行为正确"，
// probeFn/notifyLocked 走同包直调 seam（M-3，不经 worker goroutine）。
//
// 断言纪律（docs/08 §1.1，Want-Shift R2）：require=前置断言、assert=业务断言；
// 顺序敏感断言用 assert.Equal，禁 assert.ElementsMatch。

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/shengyanli1982/go-healthcheck/passive"
)

// ———————————————————————————— 测试替身（ST-08 复用契约） ————————————————————————————

// fakeClock 是手动推进的测试时钟（docs/08 §2.1：Now/Advance/Set）。
// 自带互斥锁，可在 -race 门禁下被多 goroutine 安全共享。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// 编译期断言：fake 替身满足对应消费者接口。
var (
	_ Clock                   = (*fakeClock)(nil)
	_ checker.Checker         = (*fakeChecker)(nil)
	_ hostScheduler           = (*fakeScheduler)(nil)
	_ passive.PassiveDetector = (*fakePassiveDetector)(nil)
)

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1700000000, 0)}
}

// Now 返回当前 fake 时间。
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance 推进时间 d。
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set 设置绝对时间。
func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// fakeCheckResult 是 fakeChecker 的单次可编程返回（docs/08 §2.2，任务卡字段名 doPanic）。
type fakeCheckResult struct {
	result  *checker.CheckResult
	err     error
	delay   time.Duration // 模拟慢探测（ST-08 复用）
	doPanic bool          // 模拟 Checker panic（recover 点 (1) 的输入）
}

// fakeChecker 是可编程 Checker 替身：按 results 序列依次返回，耗尽后重复返回最后一个。
// deadlines 记录每次 Check 观察到的 ctx deadline（存在时），供 per-host timeout 覆盖断言。
type fakeChecker struct {
	mu        sync.Mutex
	results   []fakeCheckResult
	callCount int
	deadlines []time.Time
}

// Check 实现 checker.Checker：依次消耗 results；delay 睡眠；doPanic 注入 panic。
func (c *fakeChecker) Check(ctx context.Context) (*checker.CheckResult, error) {
	c.mu.Lock()
	var fr fakeCheckResult
	if len(c.results) > 0 {
		idx := c.callCount
		if idx >= len(c.results) {
			idx = len(c.results) - 1 // 耗尽后重复返回最后一个
		}
		fr = c.results[idx]
	}
	c.callCount++
	if dl, ok := ctx.Deadline(); ok {
		c.deadlines = append(c.deadlines, dl)
	}
	c.mu.Unlock()

	if fr.delay > 0 {
		time.Sleep(fr.delay)
	}
	if fr.doPanic {
		panic("fakeChecker: injected Check panic")
	}
	return fr.result, fr.err
}

// calls 返回已发生的 Check 调用次数。
func (c *fakeChecker) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.callCount
}

// deadlineSnapshot 返回观察到的 ctx deadline 序列副本。
func (c *fakeChecker) deadlineSnapshot() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Time, len(c.deadlines))
	copy(out, c.deadlines)
	return out
}

// gatedChecker 是"停在 Check 内"的确定性阻塞替身（R4 在途重注册窗口编排专用；
// fakeChecker 只有 sleep 型 delay，无法与测试侧做通道会合，故非重复定义）：
// Check 进入即 close(entered)（sync.Once），阻塞至测试侧 close(release) 后返回预设结果。
// 双通道屏障把交错钉死在"probeFn 已出步骤 1、停在步骤 2"点，无真实时序依赖，-race 稳定。
type gatedChecker struct {
	entered chan struct{} // Check 已进入信号（close 一次）
	release chan struct{} // 测试侧注入：close 放行
	once    sync.Once
	result  *checker.CheckResult
}

// 编译期断言：满足 Checker 接口。
var _ checker.Checker = (*gatedChecker)(nil)

// Check 阻塞至 release 关闭后返回预设结果。
func (c *gatedChecker) Check(ctx context.Context) (*checker.CheckResult, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.result, nil
}

// twoPhaseGatedChecker 是"首调直通、二调门控"的两段式确定性替身
// （与 gatedChecker 非重复定义——gatedChecker 的 release 放行后所有调用直通，
// 无法表达两段行为）。两类窗口编排复用（预设 second 结果不同）：
//   - P1-2 Stop 取消伪影：second = Healthy=false + Err=context.Canceled
//     （模拟真实 Checker 在 probeCtx 被 Stop 的 cancel 连带取消时的事实化失败，
//     checker/http|tcp|grpc 三内置实现均如此返回）；
//   - P1-1 cbMu 阻塞窗口：second = 普通失败结果（在途探测正阻塞在 cbMu.Lock 时
//     被 RemoveHost 的窗口编排，见 TestProbeFn_RemoveHostWhileBlockedOnCbMu）。
//
// 第 1 次 Check 立即返回 first；第 2 次起 close(entered)（sync.Once）并阻塞至
// 测试侧 close(release)，随后返回 second。双通道屏障把交错钉死在
// "probeFn 已出步骤 1、停在步骤 2 Check 内"点，无真实时序依赖，-race 稳定。
type twoPhaseGatedChecker struct {
	entered chan struct{} // 第 2 次 Check 已进入信号（close 一次）
	release chan struct{} // 测试侧注入：close 放行
	once    sync.Once
	calls   atomic.Int32
	first   *checker.CheckResult // 第 1 次调用返回（立即放行）
	second  *checker.CheckResult // 第 2 次调用放行后返回
}

// 编译期断言：满足 Checker 接口。
var _ checker.Checker = (*twoPhaseGatedChecker)(nil)

// Check 首调立即返回 first；后续调用门控阻塞后返回 second。
func (c *twoPhaseGatedChecker) Check(ctx context.Context) (*checker.CheckResult, error) {
	if c.calls.Add(1) == 1 {
		return c.first, nil
	}
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.second, nil
}

// schedCall 是 fakeScheduler 统一时序日志的一条记录。
// kind ∈ {"schedule","cancel","run","shutdown","done","rearm"}；供顺序断言（INV-1b 等）。
// workers 仅 "run" 记录填写（TestStart_AlreadyStarted 断言 workers==配置值）。
type schedCall struct {
	kind     string
	host     string
	interval time.Duration
	offset   time.Duration
	workers  int
}

// fakeScheduler 实现 hostScheduler（6 方法），只记日志不真调度。
// panicOnRearmOnce 是库侧 panic 注入 seam：第一次 Rearm panic 后自动复原
// （TestProbeFn_LibraryPanic_FailLoud 用；panic 的那次 Rearm 不记入日志——非成功调用）。
type fakeScheduler struct {
	mu               sync.Mutex
	log              []schedCall
	shutdownErr      error
	panicOnRearmOnce bool
}

func (s *fakeScheduler) append(c schedCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, c)
}

// C2 机械同步（Schedule/Cancel/Done/Rearm 四方法）：签名收 any（预装箱 host），
// 记录前 host.(string) 断言还原——契约违反属编程错误，断言 panic fail-loud；
// schedCall.host 仍为 string，既有断言零修改。

// Schedule 记录 schedule 调用。
func (s *fakeScheduler) Schedule(host any, interval time.Duration, offset time.Duration) {
	s.append(schedCall{kind: "schedule", host: host.(string), interval: interval, offset: offset})
}

// Cancel 记录 cancel 调用。
func (s *fakeScheduler) Cancel(host any) {
	s.append(schedCall{kind: "cancel", host: host.(string)})
}

// Run 记录 run 调用（含 workers），不启动 goroutine（单测直调 probeFn seam）。
func (s *fakeScheduler) Run(ctx context.Context, workers int, probeFn func(host string)) {
	s.append(schedCall{kind: "run", workers: workers})
}

// Shutdown 记录 shutdown 调用并返回预设 shutdownErr（Stop 传播断言用）。
func (s *fakeScheduler) Shutdown(ctx context.Context) error {
	s.append(schedCall{kind: "shutdown"})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shutdownErr
}

// Done 记录 done 调用。
func (s *fakeScheduler) Done(host any) {
	s.append(schedCall{kind: "done", host: host.(string)})
}

// Rearm 记录 rearm 调用；panicOnRearmOnce 置位时第一次调用注入 panic（不记录、随后复原）。
func (s *fakeScheduler) Rearm(host any, interval time.Duration) {
	s.mu.Lock()
	if s.panicOnRearmOnce {
		s.panicOnRearmOnce = false
		s.mu.Unlock()
		panic("fakeScheduler: injected library panic on Rearm")
	}
	s.log = append(s.log, schedCall{kind: "rearm", host: host.(string), interval: interval})
	s.mu.Unlock()
}

// setShutdownErr 预设 Shutdown 返回的 error。
func (s *fakeScheduler) setShutdownErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shutdownErr = err
}

// kinds 返回调用类型序列快照（顺序断言用）。
func (s *fakeScheduler) kinds() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.log))
	for _, c := range s.log {
		out = append(out, c.kind)
	}
	return out
}

// callsOf 返回指定 kind 的调用记录快照。
func (s *fakeScheduler) callsOf(kind string) []schedCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []schedCall
	for _, c := range s.log {
		if c.kind == kind {
			out = append(out, c)
		}
	}
	return out
}

// cbMuProbeScheduler 包装 fakeScheduler：在 Done/Rearm 被调用的瞬间以 cbMu.TryLock
// 探测该 host 的 cbMu 是否空闲（C3 收尾位置钉死专用 seam）。sync.Mutex 非重入：
// 同 goroutine 持锁期间 TryLock 恒 false，故可构造性判定收尾调用点是否位于 cbMu
// 临界区内，无时序依赖、-race 稳定。探测成功即释放，不干扰后续锁获取；
// 其余 4 方法经嵌入提升直通 fakeScheduler（日志仍落 fs.log）。
type cbMuProbeScheduler struct {
	*fakeScheduler
	tryAcquire  func() bool // 测试注入：TryLock+立即释放；true = 调用瞬间 cbMu 空闲
	freeAtDone  bool        // 最近一次 Done 调用时的探测结果
	freeAtRearm bool        // 最近一次 Rearm 调用时的探测结果
}

// 编译期断言：包装器仍满足 hostScheduler（Done/Rearm 覆写，其余提升直通）。
var _ hostScheduler = (*cbMuProbeScheduler)(nil)

// Done 探测 cbMu 状态后转发记录。
func (s *cbMuProbeScheduler) Done(host any) {
	s.freeAtDone = s.tryAcquire()
	s.fakeScheduler.Done(host)
}

// Rearm 探测 cbMu 状态后转发记录。
func (s *cbMuProbeScheduler) Rearm(host any, interval time.Duration) {
	s.freeAtRearm = s.tryAcquire()
	s.fakeScheduler.Rearm(host, interval)
}

// fakeRecordReturn 是 fakePassiveDetector 的可编程 Record 返回值。
type fakeRecordReturn struct {
	flipped    bool
	newHealthy bool
}

// fakeRecordCall 记录一次 Record 调用入参（"先查 map 后 Record"断言用）。
type fakeRecordCall struct {
	host    string
	success bool
}

// fakePassiveDetector 实现 passive.PassiveDetector：
// Record 按序消耗 recordReturns，耗尽后返回 (false, true)；Reset 记录 resetCalls。
type fakePassiveDetector struct {
	mu            sync.Mutex
	recordReturns []fakeRecordReturn
	recordCalls   []fakeRecordCall
	resetCalls    []string
}

func newFakePassiveDetector() *fakePassiveDetector {
	return &fakePassiveDetector{}
}

// Record 记录调用并消耗预设返回值。
func (d *fakePassiveDetector) Record(host string, success bool) (bool, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.recordCalls = append(d.recordCalls, fakeRecordCall{host: host, success: success})
	if len(d.recordReturns) > 0 {
		r := d.recordReturns[0]
		d.recordReturns = d.recordReturns[1:]
		return r.flipped, r.newHealthy
	}
	return false, true // 耗尽后：未翻转、被动健康
}

// Reset 记录被重置的 host。
func (d *fakePassiveDetector) Reset(host string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.resetCalls = append(d.resetCalls, host)
}

// setRecordReturns 追加预设 Record 返回值（按序消耗）。
func (d *fakePassiveDetector) setRecordReturns(returns ...fakeRecordReturn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.recordReturns = append(d.recordReturns, returns...)
}

// recordCallsSnapshot 返回 Record 调用序列副本。
func (d *fakePassiveDetector) recordCallsSnapshot() []fakeRecordCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]fakeRecordCall, len(d.recordCalls))
	copy(out, d.recordCalls)
	return out
}

// resetCallsSnapshot 返回 Reset 调用序列副本。
func (d *fakePassiveDetector) resetCallsSnapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.resetCalls))
	copy(out, d.resetCalls)
	return out
}

// gatedPassiveDetector 是"停在 Record 内"的确定性阻塞替身（ERC P2-A 被动路径
// Record→cbMu 窗口编排专用；与 gatedChecker 同构——fakePassiveDetector 只有
// 可编程返回值、无法与测试侧做通道会合，故非重复定义）：
// Record 进入即 close(entered)（sync.Once），阻塞至测试侧 close(release) 后
// 返回预设 (flipped, newHealthy)；release 关闭后的后续 Record 立即返回同一预设
// （正对照复用）。双通道屏障把交错钉死在"report 已完成查找与 Record 翻转判定、
// 停在取 cbMu 之前"点，无真实时序依赖，-race 稳定。
// Reset 为 no-op（RemoveHost 清理链路可达即可，无状态需要清理）。
type gatedPassiveDetector struct {
	entered    chan struct{} // Record 已进入信号（close 一次）
	release    chan struct{} // 测试侧注入：close 放行
	once       sync.Once
	flipped    bool // Record 预设返回值 (1)
	newHealthy bool // Record 预设返回值 (2)
}

// 编译期断言：满足 PassiveDetector 接口。
var _ passive.PassiveDetector = (*gatedPassiveDetector)(nil)

// Record 阻塞至 release 关闭后返回预设翻转判定。
func (d *gatedPassiveDetector) Record(host string, success bool) (bool, bool) {
	d.once.Do(func() { close(d.entered) })
	<-d.release
	return d.flipped, d.newHealthy
}

// Reset 为 no-op。
func (d *gatedPassiveDetector) Reset(host string) {}

// gatedRealDetector 是包装真实 TimeWindowDetector 的确定性阻塞替身
// （P1-3 detector 条目复活窗口编排专用；与 gatedPassiveDetector 非重复定义——
// 后者无真实条目状态，无法观测"Reset 回收复活孤儿条目"与"重注册形态下 stale
// 翻转残余同样被回收"（R1 无条件 Reset 规格））：
// Record 进入即 close(entered)（sync.Once），阻塞至测试侧 close(release)，随后
// 委托 inner（惰性创建/翻转判定均为真实语义，窗口由注入 fakeClock 驱动）；
// release 关闭后的后续 Record 直通。Reset 委托 inner 并计数 resetCalls
// （区分 RemoveHost 与 report identity 失配路径两种 Reset 来源）。
type gatedRealDetector struct {
	inner      *passive.TimeWindowDetector
	entered    chan struct{} // Record 已进入信号（close 一次）
	release    chan struct{} // 测试侧注入：close 放行
	once       sync.Once
	mu         sync.Mutex
	resetCalls []string
}

// 编译期断言：满足 PassiveDetector 接口。
var _ passive.PassiveDetector = (*gatedRealDetector)(nil)

// Record 门控阻塞后委托真实 detector。
func (d *gatedRealDetector) Record(host string, success bool) (bool, bool) {
	d.once.Do(func() { close(d.entered) })
	<-d.release
	return d.inner.Record(host, success)
}

// Reset 计数后委托真实 detector。
func (d *gatedRealDetector) Reset(host string) {
	d.mu.Lock()
	d.resetCalls = append(d.resetCalls, host)
	d.mu.Unlock()
	d.inner.Reset(host)
}

// resetCallsSnapshot 返回 Reset 调用序列副本。
func (d *gatedRealDetector) resetCallsSnapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.resetCalls))
	copy(out, d.resetCalls)
	return out
}

// cbEvent 记录一次 StateChangeCallback 调用（含 result 指针同一性断言用）。
type cbEvent struct {
	host    string
	healthy bool
	result  *CheckResult
}

// cbCollector 是线程安全的状态回调收集器（回调可能被多 goroutine 并发调用，P0-1）。
type cbCollector struct {
	mu     sync.Mutex
	events []cbEvent
}

func (c *cbCollector) onChange(host string, healthy bool, result *CheckResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, cbEvent{host: host, healthy: healthy, result: result})
}

func (c *cbCollector) snapshot() []cbEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]cbEvent, len(c.events))
	copy(out, c.events)
	return out
}

// sinkEvent 记录一次 InternalErrorHandler 通知。
type sinkEvent struct {
	host string
	err  error
}

// sinkCollector 是线程安全的 InternalErrorHandler 收集器。
type sinkCollector struct {
	mu     sync.Mutex
	events []sinkEvent
}

func (s *sinkCollector) onInternalError(host string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, sinkEvent{host: host, err: err})
}

func (s *sinkCollector) snapshot() []sinkEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]sinkEvent, len(s.events))
	copy(out, s.events)
	return out
}

// ———————————————————————————— 测试辅助 ————————————————————————————

// newTestManager 经 New(...) 构造 HealthManager 并做测试接线：
//   - fs != nil：替换 m.sched 为 fakeScheduler，并关停 New() 构造的真实调度器
//     （workqueue NewTimerQueue 构造期启动搬运 goroutine，不关停则泄漏测试进程）；
//     注册 t.Cleanup 兜底 Stop（未 Start 时同样合法：Shutdown 回收构造期协程且幂等，ERC P2-2）。
//   - fs == nil：保留真实调度器，注册 t.Cleanup 关停之。
//     注意（R3）：此模式下避免调用 AddHost——真实调度器未 Run（无 worker 消费），
//     Schedule 入队项仅滞留无人消费（R3 后 Shutdown=立即关闭+有界 wg.Wait，
//     未 Run 时 wg 空、即刻返回 nil，不挂死）；需要 AddHost 的测试仍一律注入 fakeScheduler（SF5 确定性）。
//
// 注入 seam（任务卡）：同包测试直接写字段 m.sched/m.passive/m.clock，构造统一走 New(...) 后替换。
func newTestManager(t *testing.T, fs *fakeScheduler, opts ...Option) *HealthManager {
	t.Helper()
	m, err := New(opts...)
	require.NoError(t, err)
	require.NotNil(t, m)
	if fs != nil {
		real := m.sched
		m.sched = fs
		require.NoError(t, real.Shutdown(context.Background()))
		t.Cleanup(func() { _ = m.Stop(context.Background()) })
	} else {
		t.Cleanup(func() { _ = m.sched.Shutdown(context.Background()) })
	}
	return m
}

// panicStateChange 是注入 panic 的 StateChangeCallback（recover 点 (2) 的输入）。
func panicStateChange(host string, healthy bool, result *CheckResult) {
	panic("callback boom")
}

// ———————————————————————————— New / 生命周期 ————————————————————————————

// TestNew_InvalidConfig 是 DoD #8 的 New() 错误矩阵：非法配置 fail-fast 返回 error
// （不 panic、不静默回退默认值）；默认配置成功；被动检测器注入/默认构造（G2）；
// Want-Shift R2 显式 nil 注入与上限抽查（完整边界矩阵归 ST-06 TestValidate_Matrix）。
func TestNew_InvalidConfig(t *testing.T) {
	invalidCases := []struct {
		name      string
		opt       Option
		wantErr   string
		wantErrIs error // 哨兵身份契约（经 New 端到端传播，errors.Is 可判别）
	}{
		{"interval_zero", WithInterval(0), "interval", ErrInvalidInterval},
		{"timeout_zero", WithTimeout(0), "timeout", ErrInvalidTimeout},
		{"rise_zero", WithRise(0), "rise", ErrInvalidRise},
		{"fall_zero", WithFall(0), "fall", ErrInvalidFall},
		{"workers_zero", WithWorkers(0), "workers", ErrInvalidWorkers},
		{"spread_slots_zero", WithSpreadSlots(0), "spreadSlots", ErrInvalidSpreadSlots},
		{"passive_max_fails_zero", WithPassiveMaxFails(0), "passiveMaxFails", ErrInvalidPassiveMaxFails},
		{"passive_fail_timeout_zero", WithPassiveFailTimeout(0), "passiveFailTimeout", ErrInvalidPassiveFailTimeout},
		// Want-Shift R2：显式 nil 注入 fail-fast
		{"clock_nil", WithClock(nil), "clock", ErrNilClock},
		{"passive_detector_nil", WithPassiveDetector(nil), "passiveDetector", ErrNilPassiveDetector},
		// 上限抽查（端到端传播；完整矩阵归 ST-06）
		{"interval_over_max", WithInterval(24*time.Hour + time.Millisecond), "interval", ErrInvalidInterval},
		{"workers_over_max", WithWorkers(4097), "workers", ErrInvalidWorkers},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := New(tc.opt)
			require.Error(t, err)
			assert.Nil(t, m)
			assert.ErrorContains(t, err, tc.wantErr)
			assert.ErrorIs(t, err, tc.wantErrIs)
		})
	}

	t.Run("defaults_succeed", func(t *testing.T) {
		m := newTestManager(t, nil)
		assert.Equal(t, *newDefaultConfig(), m.config) // 默认值表逐字段一致
		_, ok := m.passive.(*passive.TimeWindowDetector)
		assert.True(t, ok, "New() should construct *passive.TimeWindowDetector by default when none is injected (G2)")
	})

	t.Run("injected_passive_detector_used", func(t *testing.T) {
		fpd := newFakePassiveDetector()
		m := newTestManager(t, nil, WithClock(newFakeClock()), WithPassiveDetector(fpd))
		got, ok := m.passive.(*fakePassiveDetector)
		require.True(t, ok, "the injected detector should be used as-is")
		assert.Same(t, fpd, got)
	})

	t.Run("nil_callbacks_legal", func(t *testing.T) {
		m := newTestManager(t, nil, WithClock(newFakeClock()), WithOnStateChange(nil), WithOnInternalError(nil))
		assert.Nil(t, m.onChange) // nil = 默认禁用
		assert.Nil(t, m.onInternalError)
	})
}

// TestStart_AlreadyStarted 断言 Start 幂等语义：首次成功、二次 error 含 already started；
// Run 以配置的 workers 启动（G3：类型断言已在 New 构造期完成，Start 只管重复启动+worker 启动）。
func TestStart_AlreadyStarted(t *testing.T) {
	fs := &fakeScheduler{}
	m := newTestManager(t, fs, WithWorkers(3), WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()))

	require.NoError(t, m.Start(context.Background()))

	err := m.Start(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, "already started")
	assert.ErrorIs(t, err, ErrAlreadyStarted) // 哨兵身份契约（docs/06 §5）

	runs := fs.callsOf("run")
	require.Len(t, runs, 1, "Run should be called exactly once")
	assert.Equal(t, 3, runs[0].workers, "workers should equal the configured value")
}

// TestStart_CanceledContext_FailFast 是 P3-3 修复的专项钉死：Start(已取消的 ctx)
// 必须 fail-fast 返回 error——旧行为静默产生永久失能的 manager（workers 经
// GetWithContext 立即全部退出，hosts 永不被探测；started=true 使重复 Start 返回
// ErrAlreadyStarted，四错误通道全部静默，零日志下无任何可观测信号）。
// 断言：(1) error 可经 errors.Is(err, context.Canceled) 判别（stdlib 惯例）且
// 消息含 "healthcheck: " 自包含前缀（docs/06 §5.1）；(2) 不启动 workers；
// (3) 校验失败不消耗一次性生命周期（started 未置位）——可用合法 ctx 重试成功；
// (4) host 保持冷启动 false（未被探测）；(5) 与 Start-after-Stop 路径不冲突：
// Stop 后 Start(canceled ctx) 仍返回 ErrStopped（stopped 校验优先）。
func TestStart_CanceledContext_FailFast(t *testing.T) {
	fs := &fakeScheduler{}
	m := newTestManager(t, fs, WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()))
	require.NoError(t, m.AddHost("ctx-host:80", WithChecker(&fakeChecker{})))

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := m.Start(canceledCtx)
	require.Error(t, err, "Start with an already-canceled ctx must fail fast (silent permanent disablement is unacceptable)")
	assert.ErrorIs(t, err, context.Canceled, "the error must stay identifiable as context.Canceled")
	assert.ErrorContains(t, err, "healthcheck: start with canceled context",
		"the message carries the self-contained package prefix plus the real context increment (docs/06 sec. 5.1 rule 3)")
	assert.Empty(t, fs.callsOf("run"), "a rejected Start must not launch workers")
	assert.False(t, m.IsHealthy("ctx-host:80"), "the host keeps its cold-start state (never probed)")

	// 校验失败不消耗一次性语义：started 未置位，可用合法 ctx 重试成功
	require.NoError(t, m.Start(context.Background()),
		"the rejected Start must not consume the one-shot lifecycle (retry with a valid ctx succeeds)")
	assert.Len(t, fs.callsOf("run"), 1, "exactly one successful Run after the retry")

	// Stop 优先级不冲突：Stop 后 Start(canceled ctx) 返回 ErrStopped（非 context.Canceled）
	require.NoError(t, m.Stop(context.Background()))
	assert.ErrorIs(t, m.Start(canceledCtx), ErrStopped,
		"the stopped check takes precedence over the canceled-ctx check (one-shot lifecycle)")
}

// TestStop_BeforeStart 断言未 Start 直接 Stop 的新语义（ERC P2-2 修复，取代旧"完全 no-op"）：
// 返回 nil 且触发 Scheduler.Shutdown（回收 New() 构造期启动的 TimerQueue 搬运 goroutine）；
// 置 stopped——此后 Start 返回 ErrStopped（生命周期一次性，与 AddHost after Stop 对齐）、
// AddHost 同样被拒；重复 Stop 幂等返回 nil（不再触发第二次 Shutdown）。
// goroutine 实际回收的集成侧证明见 TestIntegration_StopBeforeStart_NoGoroutineLeak。
func TestStop_BeforeStart(t *testing.T) {
	fs := &fakeScheduler{}
	m := newTestManager(t, fs, WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()))

	assert.NotPanics(t, func() {
		assert.NoError(t, m.Stop(context.Background()))
	})
	assert.Len(t, fs.callsOf("shutdown"), 1,
		"Stop without Start must still shut down the internal scheduler (construction-time goroutine reclaim, ERC P2-2)")

	// 生命周期一次性：Stop 后 Start fail-fast，不启动 worker
	err := m.Start(context.Background())
	require.Error(t, err, "Start after Stop should fail fast (one-shot lifecycle)")
	assert.ErrorContains(t, err, "manager stopped")
	assert.ErrorIs(t, err, ErrStopped) // 哨兵身份契约（docs/06 §5）
	assert.Empty(t, fs.callsOf("run"), "Start after Stop must not launch workers")

	// AddHost 同被拒（与 L5 P2-2a 既有语义对齐）
	assert.ErrorIs(t, m.AddHost("late:80", WithChecker(&fakeChecker{})), ErrStopped)

	// 重复 Stop 幂等：返回 nil、不重复 Shutdown
	assert.NoError(t, m.Stop(context.Background()), "a repeated Stop must be idempotent and return nil")
	assert.Len(t, fs.callsOf("shutdown"), 1, "a repeated Stop must not trigger a second Shutdown")
}

// TestStop_QueriesStillWork 断言优雅关停与降级：Start→探测翻转→Stop 成功（fs.log 含 shutdown）；
// Stop 后查询 API 仍返回有效值（hosts 不清空，docs/06 §3）；Stop 后 Start 返回 ErrStopped
// （生命周期一次性，ERC P2-2）、重复 Stop 幂等返回 nil；shutdownErr 经新实例的首次 Stop 传播
// （重复 Stop 幂等短路后不再传播，故传播断言独立实例）。
func TestStop_QueriesStillWork(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	r1 := &checker.CheckResult{Healthy: true}
	fchk := &fakeChecker{results: []fakeCheckResult{{result: r1}}}
	m := newTestManager(t, fs, WithClock(fc), WithPassiveDetector(fpd), WithRise(1))

	const host = "backend:8080"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))
	require.NoError(t, m.Start(context.Background()))
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host), "precondition: flipped to healthy after the probe")

	require.NoError(t, m.Stop(context.Background()), "shutdownErr=nil -> Stop returns nil")
	assert.NotEmpty(t, fs.callsOf("shutdown"), "Stop should call Scheduler.Shutdown")

	// 优雅降级：Stop 后查询 API 仍可用
	assert.True(t, m.IsHealthy(host))
	assert.Equal(t, []string{host}, m.GetHealthyHosts())
	assert.Same(t, r1, m.GetLastResult(host))

	// 生命周期一次性：Start after Stop → ErrStopped（旧语义为 already started，ERC P2-2 改判）
	assert.ErrorIs(t, m.Start(context.Background()), ErrStopped, "Start after Stop should fail fast with ErrStopped")
	// 重复 Stop 幂等：返回 nil、不再触发 Shutdown
	assert.NoError(t, m.Stop(context.Background()), "a repeated Stop must be idempotent and return nil")
	assert.Len(t, fs.callsOf("shutdown"), 1, "a repeated Stop must not trigger a second Shutdown")

	// Stop 传播 shutdownErr（新实例的首次 Stop；重复 Stop 幂等短路不再传播）
	fs2 := &fakeScheduler{}
	m2 := newTestManager(t, fs2, WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()))
	require.NoError(t, m2.Start(context.Background()))
	errShutdown := errors.New("shutdown timeout")
	fs2.setShutdownErr(errShutdown)
	err := m2.Stop(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, errShutdown)
	assert.NoError(t, m2.Stop(context.Background()), "after a failed first Stop, a repeated Stop still returns nil (idempotent)")
}

// TestAddHost_AfterStop_ReturnsError 是 L5 P2-2(a) 的生命周期 fail-fast 钉住测试：
// Start→Stop 后 AddHost 返回含 "manager stopped" 的 error。旧行为为确定性误用被静默吞掉
// （注册成功、可查询、冷启动 false，但 Schedule 的 PutAfter 因队列已关被良性忽略，
// host 永不进入探测链，零日志下四错误通道全部静默）。
// 对照组：新实例 Start 后 AddHost 正常成功（stopped 为实例级状态，不污染正常路径）。
// 注：AddHost×Stop 并发竞态窗口仍由 Schedule 的 ErrQueueIsClosed 良性忽略兜底
// （docs/04 SF3），本测试只钉住"Stop 已完成 → AddHost 拒绝"的确定性顺序。
func TestAddHost_AfterStop_ReturnsError(t *testing.T) {
	fs := &fakeScheduler{}
	m := newTestManager(t, fs, WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()))

	require.NoError(t, m.Start(context.Background()))
	require.NoError(t, m.Stop(context.Background()))

	err := m.AddHost("late:80", WithChecker(&fakeChecker{}))
	require.Error(t, err, "AddHost after Stop should fail fast with an error")
	assert.ErrorContains(t, err, "manager stopped")
	assert.ErrorIs(t, err, ErrStopped) // 哨兵身份契约（docs/06 §5）

	m.mu.RLock()
	_, registered := m.hosts["late:80"]
	m.mu.RUnlock()
	assert.False(t, registered, "a rejected host should not be registered into hosts")
	assert.Empty(t, fs.callsOf("schedule"), "a rejected host should not enter scheduling (no Schedule call)")

	// 对照：新实例 Start 后 AddHost 正常成功
	fs2 := &fakeScheduler{}
	m2 := newTestManager(t, fs2, WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()))
	require.NoError(t, m2.Start(context.Background()))
	require.NoError(t, m2.AddHost("fresh:80", WithChecker(&fakeChecker{})), "AddHost should succeed after Start on a fresh instance")
	assert.Len(t, fs2.callsOf("schedule"), 1, "the control group should enter scheduling normally")
}

// ———————————————————————————— AddHost / RemoveHost ————————————————————————————

// TestAddHost_Errors 是 DoD #8 的 AddHost 错误矩阵 + Checker/Protocol 解析优先级：
// 空 host / 重复 / 二者皆无 / 协议未注册各返回明确 error；WithChecker > WithProtocol；
// Want-Shift R2：host 级 interval/timeout 范围经 hcfg.validate(host)（ST-06 契约）。
func TestAddHost_Errors(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	m := newTestManager(t, fs, WithClock(fc), WithPassiveDetector(fpd))

	t.Run("empty_host", func(t *testing.T) {
		err := m.AddHost("", WithChecker(&fakeChecker{}))
		require.Error(t, err)
		assert.ErrorContains(t, err, "empty host")
		assert.ErrorIs(t, err, ErrEmptyHost)
	})

	t.Run("neither_checker_nor_protocol", func(t *testing.T) {
		err := m.AddHost("a:80")
		require.Error(t, err)
		assert.ErrorContains(t, err, "neither checker nor protocol")
		assert.ErrorIs(t, err, ErrNoChecker)
	})

	t.Run("protocol_not_registered", func(t *testing.T) {
		err := m.AddHost("b:80", WithProtocol("nope"))
		require.Error(t, err)
		assert.ErrorContains(t, err, "not registered")
		// checker 哨兵经 AddHost 的 "%w (host %q)" 后缀上下文包装（%w 链）后仍可判别
		assert.ErrorIs(t, err, checker.ErrProtocolNotRegistered)
	})

	t.Run("duplicate", func(t *testing.T) {
		require.NoError(t, m.AddHost("dup:80", WithChecker(&fakeChecker{})))
		err := m.AddHost("dup:80", WithChecker(&fakeChecker{}))
		require.Error(t, err)
		assert.ErrorContains(t, err, "already exists")
		assert.ErrorIs(t, err, ErrHostExists)
	})

	t.Run("host_interval_over_max", func(t *testing.T) {
		err := m.AddHost("iv:80", WithChecker(&fakeChecker{}), WithHostInterval(24*time.Hour+time.Hour))
		require.Error(t, err)
		assert.ErrorContains(t, err, "interval")
		assert.ErrorIs(t, err, ErrInvalidInterval) // host 级复用全局哨兵
	})

	t.Run("host_timeout_negative", func(t *testing.T) {
		err := m.AddHost("to:80", WithChecker(&fakeChecker{}), WithHostTimeout(-time.Second))
		require.Error(t, err)
		assert.ErrorContains(t, err, "timeout")
		assert.ErrorIs(t, err, ErrInvalidTimeout) // host 级复用全局哨兵
	})

	t.Run("checker_wins_over_protocol", func(t *testing.T) {
		// WithChecker + WithProtocol 同给 → 用 Checker，不查 Registry（"nope" 未注册仍成功）
		fchk := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
		require.NoError(t, m.AddHost("dual:80", WithChecker(fchk), WithProtocol("nope")))

		schedules := fs.callsOf("schedule")
		var found bool
		for _, c := range schedules {
			if c.host == "dual:80" {
				found = true
			}
		}
		assert.True(t, found, "the host should be scheduled after AddHost succeeds")

		m.probeFn(context.Background(), "dual:80")
		assert.Equal(t, 1, fchk.calls(), "probeFn should use the injected fakeChecker (Registry not consulted)")
	})
}

// TestAddHost_ColdStart 是 C2 集成侧：AddHost 后立即 IsHealthy==false（default_down 安全启动）、
// Hosts()==1、GetLastResult==nil（尚未探测）。
func TestAddHost_ColdStart(t *testing.T) {
	fs := &fakeScheduler{}
	m := newTestManager(t, fs, WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()))

	const host = "cold:80"
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{})))

	assert.False(t, m.IsHealthy(host), "cold-start IsHealthy must be false (C2)")
	assert.Equal(t, 1, m.Hosts())
	assert.Nil(t, m.GetLastResult(host))
}

// TestAddHost_OffsetDeterministic 断言确定性错峰（docs/04 §3.1，禁用随机数）：
// interval=160ms、spreadSlots=16 → h0/h1/h2 的 offset 依次 0/10ms/20ms
// （offset = interval*(index%spreadSlots)/spreadSlots）；相同注册序重跑逐字节相同。
func TestAddHost_OffsetDeterministic(t *testing.T) {
	build := func() []schedCall {
		fs := &fakeScheduler{}
		m := newTestManager(t, fs,
			WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()),
			WithInterval(160*time.Millisecond), WithSpreadSlots(16))
		fchk := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
		for _, h := range []string{"h0:80", "h1:80", "h2:80"} {
			require.NoError(t, m.AddHost(h, WithChecker(fchk)))
		}
		return fs.callsOf("schedule")
	}

	first := build()
	assert.Equal(t, []schedCall{
		{kind: "schedule", host: "h0:80", interval: 160 * time.Millisecond, offset: 0},
		{kind: "schedule", host: "h1:80", interval: 160 * time.Millisecond, offset: 10 * time.Millisecond},
		{kind: "schedule", host: "h2:80", interval: 160 * time.Millisecond, offset: 20 * time.Millisecond},
	}, first)

	second := build()
	assert.Equal(t, first, second, "rerun with the same registration order (new manager) must produce a byte-identical offset sequence")
}

// TestAddHost_IndexNotRecycled 断言注册索引单调递增不回收（docs/04 §3.2 稳定性）：
// AddHost h0,h1 → RemoveHost h0 → AddHost h2：h2 的 offset==20ms（index=2，非复用 index 0）。
func TestAddHost_IndexNotRecycled(t *testing.T) {
	fs := &fakeScheduler{}
	m := newTestManager(t, fs,
		WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()),
		WithInterval(160*time.Millisecond), WithSpreadSlots(16))
	fchk := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}

	require.NoError(t, m.AddHost("h0:80", WithChecker(fchk)))
	require.NoError(t, m.AddHost("h1:80", WithChecker(fchk)))
	require.NoError(t, m.RemoveHost("h0:80"))
	require.NoError(t, m.AddHost("h2:80", WithChecker(fchk)))

	schedules := fs.callsOf("schedule")
	require.Len(t, schedules, 3)
	assert.Equal(t, "h2:80", schedules[2].host)
	assert.Equal(t, 20*time.Millisecond, schedules[2].offset, "index=2 -> offset=160ms*2/16=20ms (indexes are not recycled)")
}

// TestAddHost_HostOverrides 断言 per-host 覆盖全局：schedule 的 interval==host 值；
// probeFn 用 host timeout（fakeChecker 观察 ctx deadline）；Rearm 亦用 host interval。
func TestAddHost_HostOverrides(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	fchk := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(fpd), WithRise(1),
		WithInterval(160*time.Millisecond), WithTimeout(50*time.Millisecond), WithSpreadSlots(16))

	const host = "ovr:80"
	const hostInterval = 80 * time.Millisecond
	const hostTimeout = 30 * time.Millisecond
	require.NoError(t, m.AddHost(host, WithChecker(fchk), WithHostInterval(hostInterval), WithHostTimeout(hostTimeout)))

	schedules := fs.callsOf("schedule")
	require.Len(t, schedules, 1)
	assert.Equal(t, hostInterval, schedules[0].interval, "schedule should use the host-level interval override")

	before := time.Now()
	m.probeFn(context.Background(), host)
	after := time.Now()

	rearms := fs.callsOf("rearm")
	require.Len(t, rearms, 1)
	assert.Equal(t, hostInterval, rearms[0].interval, "Rearm should use the host-level interval override")

	deadlines := fchk.deadlineSnapshot()
	require.Len(t, deadlines, 1)
	assert.False(t, deadlines[0].Before(before.Add(hostTimeout)), "probeCtx deadline should be based on the host-level timeout (not before before+30ms)")
	assert.False(t, deadlines[0].After(after.Add(hostTimeout)), "probeCtx deadline should be based on the host-level timeout (not after after+30ms; misusing the global 50ms would exceed it)")

	assert.True(t, m.IsHealthy(host))
}

// TestRemoveHost_NotFound 断言移除不存在的 host 返回 error 含 not found。
func TestRemoveHost_NotFound(t *testing.T) {
	m := newTestManager(t, &fakeScheduler{}, WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()))

	err := m.RemoveHost("ghost:80")
	require.Error(t, err)
	assert.ErrorContains(t, err, "not found")
	assert.ErrorIs(t, err, ErrHostNotFound) // 哨兵身份契约（docs/06 §5）
}

// TestRemoveHost_Cleanup 断言移除清理链路：Cancel + passive.Reset + map 清理；
// 随后 probeFn 直调（墓碑 INV-3）：仅追加 done、无 rearm（INV-2 defer 收尾不泄漏 processing）。
func TestRemoveHost_Cleanup(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	m := newTestManager(t, fs, WithClock(fc), WithPassiveDetector(fpd))

	const host = "rm:80"
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{})))
	require.NoError(t, m.RemoveHost(host))

	cancels := fs.callsOf("cancel")
	require.Len(t, cancels, 1)
	assert.Equal(t, host, cancels[0].host)
	assert.Equal(t, []string{host}, fpd.resetCallsSnapshot(), "RemoveHost should reset passive detection state (leak prevention, docs/07 Section 3)")
	assert.Equal(t, 0, m.Hosts())
	assert.False(t, m.IsHealthy(host))

	// 墓碑：probeFn 直调 → defer 收尾仅 Done，不 Rearm
	assert.NotPanics(t, func() { m.probeFn(context.Background(), host) })
	assert.Equal(t, []string{"schedule", "cancel", "done"}, fs.kinds(), "tombstone path: done appended and no rearm (INV-2/INV-3)")
}

// ———————————————————————————— probeFn 不变式 ————————————————————————————

// TestProbeFn_NormalFlow_DoneBeforeRearm 是 INV-1b 的专项钉死（W1 审查 P3-5：
// scheduler 层 SlowProbe 测试对 Done/Rearm 顺序颠倒无判别力，此处以 fakeScheduler
// 调用序断言收尾必须先 Done 后 Rearm）；同时断言 INV-4 指针同一性与 M-6 Timestamp 补填。
func TestProbeFn_NormalFlow_DoneBeforeRearm(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	r1 := &checker.CheckResult{Healthy: true, StatusCode: 200}
	fchk := &fakeChecker{results: []fakeCheckResult{{result: r1}}}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(fpd), WithRise(1), WithInterval(200*time.Millisecond))

	const host = "probe:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))

	m.probeFn(context.Background(), host)

	assert.True(t, m.IsHealthy(host), "rise=1: a single success flips to healthy")

	lr := m.GetLastResult(host)
	require.NotNil(t, lr)
	assert.Same(t, r1, lr, "GetLastResult must return the same pointer given by fakeChecker (INV-4)")
	assert.Equal(t, fc.Now(), lr.Timestamp, "a zero-value Timestamp should be backfilled by the injected Clock (M-6)")

	// INV-1b 钉死：done 在 rearm 之前（顺序颠倒即 FAIL）
	assert.Equal(t, []string{"schedule", "done", "rearm"}, fs.kinds(), "teardown must call Done before Rearm (INV-1b)")
	rearms := fs.callsOf("rearm")
	require.Len(t, rearms, 1)
	assert.Equal(t, 200*time.Millisecond, rearms[0].interval, "Rearm interval should be the configured interval")
}

// TestProbeFn_Tombstone 断言墓碑路径（INV-3）：AddHost→RemoveHost→probeFn 直调，
// fs.log 仅 done 无 rearm；GetLastResult==nil；不 panic。
func TestProbeFn_Tombstone(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	fchk := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
	m := newTestManager(t, fs, WithClock(fc), WithPassiveDetector(fpd), WithRise(1))

	const host = "tomb:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))
	require.NoError(t, m.RemoveHost(host))

	assert.NotPanics(t, func() { m.probeFn(context.Background(), host) })
	assert.Equal(t, []string{"schedule", "cancel", "done"}, fs.kinds(), "tombstone: only done, no rearm")
	assert.Nil(t, m.GetLastResult(host))
	assert.Equal(t, 0, fchk.calls(), "the tombstone path must not call the Checker")
}

// TestProbeFn_ReRegisterInFlight_StaleResultTombstoned 是 R4（L5 P2-2）专项钉死：
// 探测在途（gatedChecker 阻塞在 Check，窗口钉死在步骤 2→3 之间）时
// RemoveHost→AddHost 同名重注册，新注册的 *managedHost 与旧探测步骤 1 捕获的
// 指针不同一——旧注册的探测结果必须被丢弃（视同墓碑）：
//   - 不写入新注册 HostState（无 LastResult、不翻转、无回调，冷启动保证不被污染）；
//   - 不以旧 interval Rearm（新注册自有 AddHost 的 Schedule 链，防多余紧邻探测）；
//   - Done 仍无条件调用（INV-2 不泄漏 processing）。
//
// 编排确定性：双通道屏障（entered/release）构造性钉死交错，无真实时序依赖；
// 正对照验证守卫不误伤新注册自身的探测链路。
func TestProbeFn_ReRegisterInFlight_StaleResultTombstoned(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	col := &cbCollector{}
	stale := &checker.CheckResult{Healthy: true, StatusCode: 200}
	gated := &gatedChecker{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		result:  stale,
	}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(fpd), WithRise(1), WithInterval(200*time.Millisecond),
		WithOnStateChange(col.onChange))

	const host = "rereg:80"
	require.NoError(t, m.AddHost(host, WithChecker(gated)))

	// 在途探测：goroutine 直调 probeFn，阻塞在 Check（步骤 2）；t.Fatal 限 select 防死锁
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		m.probeFn(context.Background(), host)
	}()
	select {
	case <-gated.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("gatedChecker did not enter Check within 2s")
	}

	// 窗口内：RemoveHost→AddHost 同名重注册（配置重载类模式）→ 产生新 *managedHost
	require.NoError(t, m.RemoveHost(host))
	fresh := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
	require.NoError(t, m.AddHost(host, WithChecker(fresh)))

	// 放行旧探测并等待 probeFn 返回
	close(gated.release)
	select {
	case <-probeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("probeFn did not return within 2s")
	}

	// R4 断言(1)：旧注册探测结果被丢弃——新注册 HostState 未被污染（冷启动保持）
	assert.False(t, m.IsHealthy(host), "the stale probe result must not flip the new registration's state (rise=1; without the R4 guard this would be true)")
	assert.Nil(t, m.GetLastResult(host), "the stale probe result must not be written into the new registration's LastResult")
	assert.Empty(t, col.snapshot(), "the old registration's probe must not trigger any callback")
	// R4 断言(2)：旧探测不得 Rearm（新注册自有 Schedule 链）；Done 仍无条件（INV-2）
	assert.Equal(t, []string{"schedule", "cancel", "schedule", "done"}, fs.kinds(),
		"in-flight re-registration window: the stale probe only completes Done, no Rearm (R4; INV-1b/INV-2)")

	// 正对照：新注册自身探测正常（指针同一性守卫不误伤新注册链路）
	m.probeFn(context.Background(), host)
	assert.True(t, m.IsHealthy(host), "the new registration's own probe should flip to healthy normally")
	assert.Equal(t, []string{"schedule", "cancel", "schedule", "done", "done", "rearm"}, fs.kinds(),
		"the new registration's probe tears down normally (Done before Rearm, INV-1b)")
	events := col.snapshot()
	require.Len(t, events, 1, "the new registration's flip produces exactly 1 callback")
	assert.True(t, events[0].healthy)
	assert.NotSame(t, stale, events[0].result, "the callback must carry the new registration's own probe result")
}

// TestProbeFn_ParentCtxCanceled_ResultDiscarded 是 P1-2（Stop 取消伪影计入状态机）
// 的专项确定性钉死：在途探测阻塞在 Check（步骤 2）期间取消父 ctx（internalCtx，
// 等价 Stop 第一步的 cancel——probeCtx 派生自它被连带取消），放行后 probeFn
// 必须丢弃取消伪影结果（Healthy=false + Err=context.Canceled）：
//   - 不计入 rise/fall 状态机（fall=1 时单次伪影即触发伪 DOWN 翻转 + 关停期幽灵回调）；
//   - 不写入 LastResult（"Stop 后仍可查询"的优雅降级状态不被伪影污染）；
//   - defer 收尾照常执行（Done+Rearm 均执行——INV-2 不泄漏 processing、不静默停探；
//     真实 Stop 场景 Rearm 的 PutAfter 因队列已 Shutdown 返回 ErrQueueIsClosed 良性）。
//
// 编排确定性：fall=1 使 fallCount=0 恰为阈值边缘（fall-1），伪影一旦计入必翻转；
// 双通道屏障把取消时机钉死在步骤 2 内，无真实时序依赖，-race 稳定。
func TestProbeFn_ParentCtxCanceled_ResultDiscarded(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	col := &cbCollector{}
	first := &checker.CheckResult{Healthy: true, StatusCode: 200}
	artifact := &checker.CheckResult{Healthy: false, Err: context.Canceled}
	chk := &twoPhaseGatedChecker{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		first:   first,
		second:  artifact,
	}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(fpd), WithRise(1), WithFall(1),
		WithInterval(200*time.Millisecond), WithOnStateChange(col.onChange))

	const host = "cancel-artifact:80"
	require.NoError(t, m.AddHost(host, WithChecker(chk)))

	// 第 1 轮：成功 → UP（回调#1），构成"单次取消伪影即伪翻转"的状态机前提
	parentCtx, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	m.probeFn(parentCtx, host)
	require.True(t, m.IsHealthy(host), "precondition: rise=1 flips to healthy on the first success")
	require.Len(t, col.snapshot(), 1, "precondition: exactly 1 callback from the active flip")

	// 第 2 轮：在途探测阻塞在 Check 内（步骤 2）
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		m.probeFn(parentCtx, host)
	}()
	select {
	case <-chk.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("twoPhaseGatedChecker did not enter the gated Check within 2s")
	}

	// 模拟 Stop 第一步：取消父 internalCtx（在途探测的 probeCtx 被连带取消）
	cancelParent()
	close(chk.release) // 放行：Check 返回取消伪影（Healthy=false + Err=context.Canceled）
	select {
	case <-probeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("probeFn did not return within 2s")
	}

	// P1-2 断言(1)：取消伪影不计入状态机——无伪 DOWN 翻转、无关停期幽灵回调
	assert.Len(t, col.snapshot(), 1,
		"the cancellation artifact must be discarded: no shutdown-time ghost flip callback (without the ctx.Err() guard fall=1 would flip DOWN here)")
	// P1-2 断言(2)：可查询状态不被污染——IsHealthy 保持 true、LastResult 仍为关停前的真实结果
	assert.True(t, m.IsHealthy(host),
		"the cancellation artifact must not flip the state machine (without the guard this would be false)")
	assert.Same(t, first, m.GetLastResult(host),
		"the cancellation artifact must not be written into LastResult (graceful-degradation queryable state stays unpolluted)")
	// P1-2 断言(3)：defer 收尾照常——两轮探测各执行一次 Done+Rearm（丢弃不等于跳过收尾）
	assert.Equal(t, []string{"schedule", "done", "rearm", "done", "rearm"}, fs.kinds(),
		"teardown must still run for a discarded probe: Done before Rearm on both rounds (INV-1b/INV-2)")
}

// TestProbeFn_RemoveHostWhileBlockedOnCbMu_NoGhostCallback 是 P1-1（主动路径幽灵
// 回调守卫不对称）的专项确定性钉死，与 TestProbeFn_ReRegisterInFlight_StaleResultTombstoned
// 互补（该测试把窗口钉死在步骤 2→3 之间、identity 检查之前；本测试把窗口钉死在
// "identity 检查通过之后、阻塞在 cbMu.Lock"点——检查在 cbMu 之外时该窗口上界=
// 消费者回调时长，可无界）：
//
//	(1) G（宿主 goroutine）经 report 被动翻转持 cbMu 进入慢回调（门控阻塞）；
//	(2) W（probe-worker）在途探测放行后走向 cbMu.Lock（cbMu 被 G 持有 → 到达即阻塞；
//	    RemoveHost 落地时 W 是否已到达阻塞点不影响结论——见函数内交错不变性注释）；
//	(3) 窗口内 RemoveHost 落地（不需要 cbMu）；
//	(4) G 释放 cbMu → W 进入临界区（交错不变性：恒晚于步骤 (3)）。
//
// 无临界区内复核的旧实现：W 无条件 ApplyResult 写入孤儿 HostState + notifyLocked
// 对已删除 host 投递幽灵 DOWN 翻转（与 ERC P2-A 被动路径认定的危害同类）。
// 修复形态（与 report 路径对称）：identity 复核移入 cbMu 临界区内，失配即丢弃。
func TestProbeFn_RemoveHostWhileBlockedOnCbMu_NoGhostCallback(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	fpd.setRecordReturns(fakeRecordReturn{flipped: true, newHealthy: false}) // G 的被动翻转判定
	col := &cbCollector{}
	first := &checker.CheckResult{Healthy: true, StatusCode: 200}
	second := &checker.CheckResult{Healthy: false, Err: errors.New("real failure")}
	chk := &twoPhaseGatedChecker{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		first:   first,
		second:  second,
	}

	// 慢回调门控：被动翻转回调（result==nil）持 cbMu 阻塞——模拟消费者回调时长无界
	cbEntered := make(chan struct{})
	cbRelease := make(chan struct{})
	var cbOnce sync.Once
	testCb := func(host string, healthy bool, result *CheckResult) {
		if result == nil { // 被动翻转回调特征（R-d）：阻塞点在 notifyLocked 内，cbMu 仍被持有
			cbOnce.Do(func() { close(cbEntered) })
			<-cbRelease
		}
		col.onChange(host, healthy, result)
	}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(fpd), WithRise(1), WithFall(1),
		WithInterval(200*time.Millisecond), WithOnStateChange(testCb))

	const host = "cbmu-window:80"
	require.NoError(t, m.AddHost(host, WithChecker(chk)))
	mhOld, ok := m.getManagedHost(host)
	require.True(t, ok)

	// 第 1 轮探测：成功 → UP（回调#1，result!=nil 不门控）——构成被动翻转的 active 前提
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host), "precondition: rise=1 flips to healthy")
	require.Len(t, col.snapshot(), 1, "precondition: exactly 1 callback so far")

	// (1) G：ReportFailure → Record 预设翻转 → cbMu（无竞争取得）→ identity 通过 →
	//     SetPassiveHealthy(false) 综合翻转 → notifyLocked → 慢回调门控（持 cbMu 阻塞）
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		m.ReportFailure(host)
	}()
	select {
	case <-cbEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("the gated passive-flip callback did not start within 2s")
	}

	// (2) W：在途探测阻塞在 Check（步骤 2）
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		m.probeFn(context.Background(), host)
	}()
	select {
	case <-chk.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("twoPhaseGatedChecker did not enter the gated Check within 2s")
	}
	// 放行 W 的 Check（返回真实失败，fall=1 → 无守卫则必翻转投递）
	close(chk.release)
	// 交错不变性（替代原 200ms 路径裕量 sleep——W 到达 cbMu.Lock 的时刻与断言正确性
	// 无关）：G 自 cbEntered 观测点起持续持有 cbMu，直至 close(cbRelease) 之后才随
	// report 收尾释放；RemoveHost 在测试 goroutine 程序序上先于 cbRelease 且不需要
	// cbMu。故 cbMu 的释放恒晚于 RemoveHost 完成——W 无论已阻塞在 cbMu.Lock 还是
	// G 释放后才到达，进入临界区的时刻必在 RemoveHost 之后，临界区内 identity 复核
	// （当前实现锁外不留重复检查，probeFn 步骤 3）必然失配、守卫必触发。不存在 W
	// 先于 RemoveHost 进入临界区的合法交错，下方全部断言与调度无关（无时序依赖形态）。

	// (3) 窗口内 RemoveHost 落地（不需要 cbMu；delete + Cancel + detector Reset）
	require.NoError(t, m.RemoveHost(host))

	// (4) 释放慢回调 → G 收尾并释放 cbMu → W 进入临界区（旧实现：无复核直接写+投递）
	close(cbRelease)
	select {
	case <-reportDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ReportFailure did not return within 2s")
	}
	select {
	case <-probeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("probeFn did not return within 2s")
	}

	// P1-1 断言(1)：无幽灵回调——事件恰 2 个（主动 UP#1 + 被动 DOWN#2）；
	// 旧实现（identity 在 cbMu 外、临界区内无复核）此处必为 3 个（W 的幽灵 DOWN 翻转）
	events := col.snapshot()
	require.Len(t, events, 2,
		"the blocked-then-orphaned probe must not deliver a ghost flip callback (without the in-critical-section recheck this would be 3)")
	assert.Equal(t, cbEvent{host: host, healthy: true, result: first}, events[0], "event #1: active flip up")
	assert.Equal(t, cbEvent{host: host, healthy: false, result: nil}, events[1], "event #2: passive flip down (result=nil, R-d)")
	// P1-1 断言(2)：无孤儿状态写入——W 的结果不得覆盖已删除注册 HostState 的
	// LastResult（第 1 轮探测已写入 first；旧实现下此处必为 W 的 second）
	assert.Same(t, first, mhOld.hs.LastResult(),
		"the orphaned HostState must not receive the stale probe's LastResult write (without the recheck this would be the second result)")
	// 护栏断言：收尾不泄漏（W 经 defer 收尾 Done；identity 失配不 Rearm）、
	// detector Reset 恰为 RemoveHost 的一次（G 复核通过路径与 W 主动路径均不触发 Reset）
	assert.Equal(t, []string{"schedule", "done", "rearm", "cancel", "done"}, fs.kinds(),
		"teardown: round 1 done+rearm, RemoveHost cancel, the orphaned probe only Done, no Rearm (INV-2/R4)")
	assert.Equal(t, []string{host}, fpd.resetCallsSnapshot(),
		"exactly one detector Reset, from RemoveHost itself")
}

// TestProbeFn_CheckerPanic_SafeCheck 是 recover 点 (1)/C7 基础层：Checker panic 被
// safeCheck 捕获合成 CheckResult.Err（不向外 panic），走正常 RecordFailure 路径，
// 收尾（done+rearm）仍完成——worker 不退出、host 不停探。
func TestProbeFn_CheckerPanic_SafeCheck(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	fchk := &fakeChecker{results: []fakeCheckResult{{doPanic: true}}}
	m := newTestManager(t, fs, WithClock(fc), WithPassiveDetector(fpd), WithInterval(200*time.Millisecond))

	const host = "panic:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))

	assert.NotPanics(t, func() { m.probeFn(context.Background(), host) }, "a Checker panic must be isolated by safeCheck (recover (1))")

	lr := m.GetLastResult(host)
	require.NotNil(t, lr)
	assert.False(t, lr.Healthy)
	require.NotNil(t, lr.Err)
	assert.ErrorContains(t, lr.Err, "checker panic:")
	assert.ErrorContains(t, lr.Err, "injected Check panic", "the synthesized Err should carry the panic value")
	assert.Equal(t, fc.Now(), lr.Timestamp)

	assert.Equal(t, []string{"schedule", "done", "rearm"}, fs.kinds(), "teardown still completes after panic isolation (done+rearm)")
}

// TestProbeFn_CheckerError_Synthesized 断言 (nil, error) 基础设施错误的合成路径
// （docs/06 §1.2）：Healthy==false、Err 为原 error、Timestamp 由注入 Clock 补填。
func TestProbeFn_CheckerError_Synthesized(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	boom := errors.New("boom")
	fchk := &fakeChecker{results: []fakeCheckResult{{err: boom}}}
	m := newTestManager(t, fs, WithClock(fc), WithPassiveDetector(fpd))

	const host = "err:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))

	m.probeFn(context.Background(), host)

	lr := m.GetLastResult(host)
	require.NotNil(t, lr)
	assert.False(t, lr.Healthy)
	assert.EqualError(t, lr.Err, "boom")
	assert.ErrorIs(t, lr.Err, boom)
	assert.Equal(t, fc.Now(), lr.Timestamp, "the synthesized result's Timestamp should come from the injected Clock")
}

// TestProbeFn_NilNilResult 断言 (nil, nil) 契约行为：视为 Healthy=false，不 panic
// （docs/06 §4：契约已写明，非防御性编程）。
func TestProbeFn_NilNilResult(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	fchk := &fakeChecker{results: []fakeCheckResult{{}}} // (nil, nil)
	m := newTestManager(t, fs, WithClock(fc), WithPassiveDetector(fpd))

	const host = "nilnil:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))

	assert.NotPanics(t, func() { m.probeFn(context.Background(), host) })

	lr := m.GetLastResult(host)
	require.NotNil(t, lr)
	assert.False(t, lr.Healthy)
	assert.Nil(t, lr.Err)
	assert.Equal(t, fc.Now(), lr.Timestamp)
}

// TestProbeFn_LibraryPanic_FailLoud 是 docs/08 §3.5 行 9-10 的 fail-loud + defer 收尾断言：
// 库侧 panic（Rearm 注入）不被库 recover、向上传播（决策 A）；panic unwind 期间
// defer 收尾仍完成 Done+Rearm（不泄漏 processing、不停探、不阻塞 Stop 的有界等待）；
// finished 两段式语义（S2）：panic 发生在步骤 5 Rearm → defer 重做 Done+Rearm。
func TestProbeFn_LibraryPanic_FailLoud(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{panicOnRearmOnce: true} // 库侧 panic 注入 seam：第一次 Rearm panic 后自动复原
	fpd := newFakePassiveDetector()
	fchk := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
	m := newTestManager(t, fs, WithClock(fc), WithPassiveDetector(fpd), WithRise(1), WithInterval(200*time.Millisecond))

	const host = "failoud:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))

	recovered := func() (r any) {
		defer func() { r = recover() }()
		m.probeFn(context.Background(), host)
		return nil
	}()
	assert.NotNil(t, recovered, "a library-side panic must propagate upward (fail-loud) and not be recovered by the library")

	// 步骤 5：Done(#1) → Rearm panic（未成功，不记录）→ unwind：defer 重做 Done(#2)+Rearm（成功）
	assert.Equal(t, []string{"schedule", "done", "done", "rearm"}, fs.kinds(),
		"defer teardown must complete Done+Rearm during panic unwind (>= 1 done, exactly 1 successful rearm)")
}

// TestProbeFn_DoneRearmOutsideCbMu 是 C3（性能优化）的正常路径钉死：步骤 5 的
// Done/Rearm 必须在 cbMu 临界区之外执行——消除「cbMu → workqueue 全局锁」双重
// 排队（r5 §2.1：cbMu 锁延迟占 ParallelHosts 31.3%）。判定手段：Done/Rearm 调用
// 瞬间以 TryLock 探测该 host 的 cbMu（非重入：同 goroutine 持锁期间恒 false），
// 空闲即收尾已移出临界区——构造性断言，无时序依赖。panic 路径（外层 defer 收尾）
// 今天即在 cbMu 外，C3 使正常路径与其收敛一致
// （见 TestProbeFn_PanicInCriticalSection_CbMuUnlockedExactlyOnce）。
func TestProbeFn_DoneRearmOutsideCbMu(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	probe := &cbMuProbeScheduler{fakeScheduler: fs}
	fpd := newFakePassiveDetector()
	fchk := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true, StatusCode: 200}}}}
	m := newTestManager(t, fs, WithClock(fc), WithPassiveDetector(fpd), WithRise(1), WithInterval(200*time.Millisecond))
	m.sched = probe // 包装 fakeScheduler：Done/Rearm 处探测锁状态（调用记录仍落 fs.log）

	const host = "c3-teardown:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))
	mh, ok := m.getManagedHost(host)
	require.True(t, ok)
	probe.tryAcquire = func() bool {
		if mh.cbMu.TryLock() {
			mh.cbMu.Unlock() // 探测成功即释放：不干扰正常路径的后续锁获取
			return true
		}
		return false
	}

	m.probeFn(context.Background(), host)

	// C3 核心断言：步骤 5 收尾在 cbMu 临界区之外（旧形态步骤 5 持 cbMu，两断言必 false）
	assert.True(t, probe.freeAtDone,
		"step-5 Done must run outside the cbMu critical section (C3: eliminates the cbMu -> workqueue-global-lock nested queueing)")
	assert.True(t, probe.freeAtRearm,
		"step-5 Rearm must run outside the cbMu critical section (C3)")
	// 守护：收尾链与状态机语义零变化（INV-1b 先 Done 后 Rearm；步骤 3+4 照常写入）
	assert.Equal(t, []string{"schedule", "done", "rearm"}, fs.kinds(), "teardown chain intact: Done before Rearm (INV-1b)")
	assert.True(t, m.IsHealthy(host), "rise=1: the probe result is still applied normally (steps 3+4 semantics unchanged)")
}

// TestProbeFn_PanicInCriticalSection_CbMuUnlockedExactlyOnce 是 C3 的 panic 路径
// 钉死（R-1：提前 return 与 panic 路径必须恰好 unlock 一次）。注入链：rise=1 翻转
// → 回调 panic（"callback boom"）→ notifyLocked recover → sink panic（"sink boom"，
// sink 不被 recover，fail-loud）→ panic 在 cbMu 临界区内（步骤 4）向上传播。断言：
//   - 传播的 panic 恰为注入值——双重 unlock 会以 "sync: unlock of unlocked mutex"
//     替换传播值，泄漏 unlock 则后续探测永久死锁（超时 FAIL）；
//   - unwind 期间外层 defer 收尾完成 Done+Rearm 且执行时 cbMu 已释放（INV-2/S2）；
//   - 恢复后再次探测正常（cbMu 可再次获取，收尾链完整）。
func TestProbeFn_PanicInCriticalSection_CbMuUnlockedExactlyOnce(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	probe := &cbMuProbeScheduler{fakeScheduler: fs}
	fpd := newFakePassiveDetector()
	fchk := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
	sinkPanic := func(host string, err error) { panic("sink boom") }
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(fpd), WithRise(1), WithInterval(200*time.Millisecond),
		WithOnStateChange(panicStateChange), WithOnInternalError(sinkPanic))
	m.sched = probe

	const host = "c3-panic-unlock:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))
	mh, ok := m.getManagedHost(host)
	require.True(t, ok)
	probe.tryAcquire = func() bool {
		if mh.cbMu.TryLock() {
			mh.cbMu.Unlock()
			return true
		}
		return false
	}

	recovered := func() (r any) {
		defer func() { r = recover() }()
		m.probeFn(context.Background(), host)
		return nil
	}()

	// R-1 断言(1)：传播 panic 恰为注入值（双重 unlock 的实现错误会替换该值）
	assert.Equal(t, "sink boom", recovered,
		"the injected sink panic must propagate unchanged (a double-unlock bug would surface as 'sync: unlock of unlocked mutex')")
	// R-1 断言(2)：unwind 收尾完成且在 cbMu 之外（finished=false → defer 兜底 Done+Rearm）
	assert.Equal(t, []string{"schedule", "done", "rearm"}, fs.kinds(),
		"defer teardown must complete Done+Rearm during panic unwind (INV-2/S2)")
	assert.True(t, probe.freeAtDone,
		"teardown Done during unwind must run with cbMu already released (exactly-once unlock)")
	assert.True(t, probe.freeAtRearm,
		"teardown Rearm during unwind must run with cbMu already released (exactly-once unlock)")
	// R-1 断言(3)：unlock 无泄漏——后续探测仍能取得 cbMu（泄漏持锁则此处永久死锁）；
	// 第 2 轮已 UP 无翻转 → 无回调 → 无 panic，正常收尾
	assert.NotPanics(t, func() { m.probeFn(context.Background(), host) },
		"a subsequent probe must not deadlock on a leaked cbMu (steady state: no flip, no callback)")
	assert.Equal(t, []string{"schedule", "done", "rearm", "done", "rearm"}, fs.kinds(),
		"the recovered probe tears down normally (Done before Rearm, INV-1b)")
}

// TestProbeFn_CallbackFired 断言翻转回调投递：rise=1 成功翻转恰收 (host,true,result)
// 且 result 指针==GetLastResult（INV-4）；fall=1 失败翻转恰收 (host,false,result)；
// 未翻转的探测不追加回调。
func TestProbeFn_CallbackFired(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	col := &cbCollector{}
	r1 := &checker.CheckResult{Healthy: true}
	r2 := &checker.CheckResult{Healthy: false, Err: errors.New("down")}
	r3 := &checker.CheckResult{Healthy: false, Err: errors.New("down again")}
	fchk := &fakeChecker{results: []fakeCheckResult{{result: r1}, {result: r2}, {result: r3}}}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(fpd), WithRise(1), WithFall(1), WithOnStateChange(col.onChange))

	const host = "cb:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))

	// 探测 1：DOWN→UP 翻转，恰收 1 次 (host,true,r1)
	m.probeFn(context.Background(), host)
	events := col.snapshot()
	require.Len(t, events, 1)
	assert.Equal(t, host, events[0].host)
	assert.True(t, events[0].healthy)
	assert.Same(t, r1, events[0].result)
	assert.Same(t, m.GetLastResult(host), events[0].result, "callback result pointer == GetLastResult (INV-4)")

	// 探测 2：UP→DOWN 翻转（fall=1），恰收 (host,false,r2)
	m.probeFn(context.Background(), host)
	events = col.snapshot()
	require.Len(t, events, 2)
	assert.Equal(t, host, events[1].host)
	assert.False(t, events[1].healthy)
	assert.Same(t, r2, events[1].result)

	// 探测 3：已 DOWN 再失败 → 不翻转 → 不追加回调
	m.probeFn(context.Background(), host)
	events = col.snapshot()
	assert.Len(t, events, 2, "probes that do not flip must not append callbacks")
}

// ———————————————————————————— notifyLocked（recover 点 (2)） ————————————————————————————

// TestNotifyLocked_PanicToSink 是 recover 点 (2)/DoD #11 单元层：回调 panic 被隔离，
// 路由到 InternalErrorHandler（恰 1 次，err 含固定前缀）；notifyLocked 正常返回。
// 前置条件：调用者已持该 host 的 cbMu（sync.Mutex 非重入，notifyLocked 不再 Lock）。
func TestNotifyLocked_PanicToSink(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	sink := &sinkCollector{}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(fpd),
		WithOnStateChange(panicStateChange), WithOnInternalError(sink.onInternalError))

	const host = "sink:80"
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{})))
	mh, ok := m.getManagedHost(host)
	require.True(t, ok)

	mh.cbMu.Lock()
	assert.NotPanics(t, func() { m.notifyLocked(host, true, nil) }, "a callback panic must be isolated by notifyLocked (recover (2))")
	mh.cbMu.Unlock()

	events := sink.snapshot()
	require.Len(t, events, 1, "sink receives exactly 1 notification")
	assert.Equal(t, host, events[0].host)
	assert.ErrorContains(t, events[0].err, "StateChangeCallback panic for host")
	assert.ErrorContains(t, events[0].err, "callback boom", "err should carry the panic value")
}

// TestNotifyLocked_PanicNilSinkDropped 断言 sink=nil 时回调 panic 被隔离并丢弃
// （文档化声明，docs/06 §1.3）：notifyLocked 正常返回、不 panic。
func TestNotifyLocked_PanicNilSinkDropped(t *testing.T) {
	m := newTestManager(t, &fakeScheduler{},
		WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()),
		WithOnStateChange(panicStateChange)) // 不注入 sink

	const host = "dropsink:80"
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{})))
	mh, ok := m.getManagedHost(host)
	require.True(t, ok)

	mh.cbMu.Lock()
	assert.NotPanics(t, func() { m.notifyLocked(host, true, nil) }, "sink=nil: the panic is isolated and dropped")
	mh.cbMu.Unlock()
}

// TestNotifyLocked_NilOnChange 断言 onChange=nil 时直接 return：不投递、不调 sink。
func TestNotifyLocked_NilOnChange(t *testing.T) {
	sink := &sinkCollector{}
	m := newTestManager(t, &fakeScheduler{},
		WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()),
		WithOnInternalError(sink.onInternalError)) // onChange 保持 nil

	const host = "nocb:80"
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{})))
	mh, ok := m.getManagedHost(host)
	require.True(t, ok)

	mh.cbMu.Lock()
	assert.NotPanics(t, func() { m.notifyLocked(host, true, nil) })
	mh.cbMu.Unlock()

	assert.Empty(t, sink.snapshot(), "onChange=nil: returns directly, sink not called")
}

// ———————————————————————————— 被动上报 ————————————————————————————

// TestReport_PassiveFlip_Callback 断言被动翻转链路（锁序 W3 + R-d）：
// 主动置 UP 后，detector 判死翻转 → 回调恰收 (host,false,nil)（被动翻转 result==nil）；
// detector 恢复翻转 → ReportSuccess → 回调收 (host,true,nil)。
func TestReport_PassiveFlip_Callback(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	col := &cbCollector{}
	r1 := &checker.CheckResult{Healthy: true}
	fchk := &fakeChecker{results: []fakeCheckResult{{result: r1}}}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(fpd), WithRise(1), WithOnStateChange(col.onChange))

	const host = "passive:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))

	// 主动置 UP（rise=1）：收 (host,true,r1)
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host))

	// detector 判死翻转 → ReportFailure → 综合翻转 DOWN，回调 result==nil（R-d）
	fpd.setRecordReturns(fakeRecordReturn{flipped: true, newHealthy: false})
	m.ReportFailure(host)
	events := col.snapshot()
	require.Len(t, events, 2)
	assert.Equal(t, cbEvent{host: host, healthy: false, result: nil}, events[1], "a passive flip callback's result must be nil (R-d)")
	assert.False(t, m.IsHealthy(host))

	// detector 恢复翻转 → ReportSuccess → 综合翻转 UP
	fpd.setRecordReturns(fakeRecordReturn{flipped: true, newHealthy: true})
	m.ReportSuccess(host)
	events = col.snapshot()
	require.Len(t, events, 3)
	assert.Equal(t, cbEvent{host: host, healthy: true, result: nil}, events[2])
	assert.True(t, m.IsHealthy(host))

	// Record 入参正确：ReportFailure→success=false、ReportSuccess→success=true
	assert.Equal(t, []fakeRecordCall{{host: host, success: false}, {host: host, success: true}}, fpd.recordCallsSnapshot())
}

// TestReport_UnknownHost_Silent 断言未知 host 静默返回（docs/05 §5 热路径不校验）：
// 不 panic、无回调、fpd 未被调用（先查 map 后 Record）。
func TestReport_UnknownHost_Silent(t *testing.T) {
	fpd := newFakePassiveDetector()
	col := &cbCollector{}
	m := newTestManager(t, &fakeScheduler{},
		WithClock(newFakeClock()), WithPassiveDetector(fpd), WithOnStateChange(col.onChange))

	assert.NotPanics(t, func() {
		m.ReportSuccess("ghost:80")
		m.ReportFailure("ghost:80")
	})
	assert.Empty(t, col.snapshot(), "an unknown host must not trigger callbacks")
	assert.Empty(t, fpd.recordCallsSnapshot(), "an unknown host must not call detector.Record (map lookup before Record)")
}

// TestReport_ReRegisterInFlight_StaleReportDropped 是 ERC P2-A（被动路径对称守卫）
// 的专项确定性钉死（与 R4 主动路径测试 TestProbeFn_ReRegisterInFlight_StaleResultTombstoned
// 同款编排）：被动上报在途（gatedPassiveDetector 阻塞在 Record，窗口钉死在
// 翻转判定之后、取 cbMu 之前）时 RemoveHost→AddHost 同名重注册——report 取得
// cbMu 后以指针同一性复核发现 hosts[host] 已指向新 *managedHost，必须丢弃本次上报：
//   - 不投递幽灵回调（无守卫的旧设计：旧注册综合状态已 UP，SetPassiveHealthy(false)
//     必然综合翻转 → 对已移除/同名重注册的 host 投递 healthy=false 幽灵事件，
//     且与新注册 IsHealthy==false 冷启动查询自相矛盾，破坏 N1 镜像自洽前提）；
//   - 不写旧注册 HostState 的镜像 bool（PassiveHealthy 保持 true）；
//   - 不污染新注册（冷启动保持）。
//
// 编排确定性：双通道屏障（entered/release）构造性钉死交错，无真实时序依赖；
// 正对照验证守卫不误伤新注册自身的主动/被动翻转链路。
func TestReport_ReRegisterInFlight_StaleReportDropped(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	col := &cbCollector{}
	gated := &gatedPassiveDetector{
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
		flipped:    true,
		newHealthy: false, // 预设判死翻转
	}
	r1 := &checker.CheckResult{Healthy: true}
	fchk := &fakeChecker{results: []fakeCheckResult{{result: r1}}}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(gated), WithRise(1),
		WithOnStateChange(col.onChange))

	const host = "passive-rereg:80"
	require.NoError(t, m.AddHost(host, WithChecker(fchk)))
	mhOld, ok := m.getManagedHost(host)
	require.True(t, ok)
	// 主动探测将旧注册综合置 UP（rise=1，回调#1: true+r1）——去守卫后幽灵回调必发
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host), "precondition: old registration is combined-healthy before the stale report")
	require.Len(t, col.snapshot(), 1, "precondition: exactly 1 callback from the active flip")

	// 在途被动上报：goroutine 直调 ReportFailure，阻塞在 Record 内（查找已完成、停在取 cbMu 前）
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		m.ReportFailure(host)
	}()
	select {
	case <-gated.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("gatedPassiveDetector did not enter Record within 2s")
	}

	// 窗口内：RemoveHost（墓碑+passive.Reset）→ AddHost 同名重注册 → 产生新 *managedHost
	require.NoError(t, m.RemoveHost(host))
	fresh := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
	require.NoError(t, m.AddHost(host, WithChecker(fresh)))

	// 放行旧上报并等待返回
	close(gated.release)
	select {
	case <-reportDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ReportFailure did not return within 2s")
	}

	// P2-A 断言(1)：旧上报被指针同一性复核丢弃——无幽灵回调（去掉守卫此处必为 2 个事件）
	assert.Len(t, col.snapshot(), 1,
		"the stale report must be dropped by the pointer-identity recheck: no ghost callback (without the guard this would be 2)")
	// P2-A 断言(2)：旧注册 HostState 未被写入（PassiveHealthy 保持 true；mhOld 已不可达，同包直读）
	assert.True(t, mhOld.hs.IsHealthy(),
		"the stale report must not write the old registration's PassiveHealthy mirror (without the guard this would be false)")
	// P2-A 断言(3)：新注册未被污染（冷启动保持）
	assert.False(t, m.IsHealthy(host), "the new registration must keep its cold-start state (not polluted by the stale report)")
	assert.Nil(t, m.GetLastResult(host), "the new registration's LastResult must remain nil")

	// 正对照：新注册自身链路正常（守卫不误伤当前注册）
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host), "the new registration's own probe should flip to healthy normally")
	m.ReportFailure(host) // gated 已放行：Record 立即返回预设 (flipped=true, newHealthy=false)
	events := col.snapshot()
	require.Len(t, events, 3, "the new registration's own active flip (#2) and passive flip (#3) deliver exactly 2 more callbacks")
	assert.True(t, events[1].healthy, "event #2 is the new registration's active flip to healthy")
	assert.Equal(t, cbEvent{host: host, healthy: false, result: nil}, events[2],
		"the new registration's passive flip delivers normally (result=nil, R-d)")
	assert.False(t, m.IsHealthy(host), "final: passive flip down applied to the new registration")
}

// TestReport_RemoveHostRace_OrphanEntryReclaimed 是 P1-3（RemoveHost×report 竞态
// 复活 detector 条目 → 被动层静默失效 + 永久泄漏）的确定性 oracle：
// 模拟竞态交错——report 已通过 lookup、阻塞在 Record 门口时 RemoveHost 完成
// delete+Reset；放行后 Record 惰性重建**孤儿条目**并翻转（maxFails=1 单次失败
// 即翻转为 passiveHealthy=false）。修复要求：report 在 cbMu 临界区内 identity
// 复核发现 host 确已不存在 → 主动 m.passive.Reset(host) 回收孤儿条目。
//
// 判别 oracle（可经导出行为观测）：同名重注册后被动失败上报必须能正常翻转——
// 无修复时孤儿条目卡在 passiveHealthy=false（失败路径对已判死 host 永不翻转，
// timewindow.go），新注册镜像恒 true → 被动检测对该 host 静默失效（IsHealthy
// 恒 true、无翻转回调），且 detector map 条目永不回收。
func TestReport_RemoveHostRace_OrphanEntryReclaimed(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	col := &cbCollector{}
	gated := &gatedRealDetector{
		inner:   passive.NewTimeWindowDetector(1, time.Minute, fc), // maxFails=1：复活条目单次失败即翻转
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	healthy := &checker.CheckResult{Healthy: true}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(gated), WithRise(1), WithFall(1),
		WithOnStateChange(col.onChange))

	const host = "orphan-entry:80"
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{results: []fakeCheckResult{{result: healthy}}})))
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host), "precondition: old registration probed UP")

	// 在途 report：阻塞在 Record 门口（lookup 已通过、持有旧 *managedHost）
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		m.ReportFailure(host)
	}()
	select {
	case <-gated.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("gatedRealDetector did not enter Record within 2s")
	}

	// 竞态交错：RemoveHost 在 Record 之前落地（delete + Cancel + Reset——此刻无条目，Reset 无害）
	require.NoError(t, m.RemoveHost(host))

	// 放行：Record 惰性重建孤儿条目并翻转（maxFails=1）→ report 进入 cbMu → identity 失配
	close(gated.release)
	select {
	case <-reportDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ReportFailure did not return within 2s")
	}

	// P1-3 断言(1)：identity 失配且 host 确已不存在 → report 路径主动 Reset 回收孤儿条目
	// （resetCalls = RemoveHost 一次 + report 失配路径一次）
	assert.Equal(t, []string{host, host}, gated.resetCallsSnapshot(),
		"the identity-mismatch path must Reset the resurrected orphan entry (without the fix only RemoveHost's Reset is recorded)")
	// 失配 report 不投递（既有 ERC P2-A 守卫保持）
	assert.Len(t, col.snapshot(), 1, "the stale report delivers no ghost callback (only the precondition active flip)")

	// P1-3 断言(2)（判别 oracle）：同名重注册后被动失败能正常翻转——
	// 无修复时孤儿条目 passiveHealthy=false 永不翻转 → 被动检测静默失效
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{results: []fakeCheckResult{{result: healthy}}})))
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host), "precondition: fresh registration probed UP")
	m.ReportFailure(host)
	assert.False(t, m.IsHealthy(host),
		"passive detection must work for the fresh registration: a leaked orphan entry (passiveHealthy=false, never flips again) would silently swallow this failure and leave IsHealthy true")
	events := col.snapshot()
	require.Len(t, events, 3, "the fresh registration's passive flip delivers exactly 1 more callback")
	assert.Equal(t, cbEvent{host: host, healthy: false, result: nil}, events[2],
		"the passive flip event carries result=nil (R-d)")
}

// TestReport_ReRegisterInFlight_StaleFlipReclaimedByReset 是 P1-3 修复在
// **重注册形态**下的规格钉死（ERC Phase4 R1 裁定：失配无条件 Reset，撤销
// 原"重注册豁免 Reset"两形态分支）：report identity 复核失配但 host 存在
// （RemoveHost→AddHost 同名重注册，指针不同）→ stale 报告的 Record 在 identity
// 复核**之前**执行，其翻转残余已污染同名条目（镜像 true / detector false 分歧，
// 持续失败流量下无界存续 = 审计 P1-3 危害链）→ 必须同样 Reset 回收，仅丢弃投递。
// 误清成本有界瞬态（新世代计数从 0 重建，至多延迟 maxFails-1 次上报），方向安全
// （false-down）；豁免 Reset 的成本是 false-up 分歧无限期存续——危险不对称。
// 行为级判别（Reset 后新世代被动检测存活）见姊妹测试
// TestReport_ReRegisterStaleFlip_NewGenerationPassiveDetectionAlive。
func TestReport_ReRegisterInFlight_StaleFlipReclaimedByReset(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	col := &cbCollector{}
	gated := &gatedRealDetector{
		inner:   passive.NewTimeWindowDetector(1, time.Minute, fc),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	healthy := &checker.CheckResult{Healthy: true}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(gated), WithRise(1), WithFall(1),
		WithOnStateChange(col.onChange))

	const host = "stale-flip-reset:80"
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{results: []fakeCheckResult{{result: healthy}}})))
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host), "precondition: old registration probed UP")

	// 在途 report 阻塞在 Record 门口（lookup 已通过、持有旧 *managedHost）
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		m.ReportFailure(host)
	}()
	select {
	case <-gated.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("gatedRealDetector did not enter Record within 2s")
	}

	// 窗口内：RemoveHost → AddHost 同名重注册（新 *managedHost；新注册探测器立即 UP）
	require.NoError(t, m.RemoveHost(host))
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{results: []fakeCheckResult{{result: healthy}}})))
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host), "precondition: fresh registration probed UP")

	// 放行：旧 report 的 Record 对同名条目惰性重建并翻转 → cbMu → identity 失配
	// （存在但指针不同）→ R1 规格：无条件 Reset 回收 stale 翻转残余 + 丢弃投递
	close(gated.release)
	select {
	case <-reportDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ReportFailure did not return within 2s")
	}

	// R1 断言(1)：resetCalls = RemoveHost 一次 + report 失配路径一次——重注册形态不豁免
	assert.Equal(t, []string{host, host}, gated.resetCallsSnapshot(),
		"the identity-mismatch path must Reset the stale flip even with a live re-registration (without the R1 fix only RemoveHost's Reset is recorded)")
	// R1 断言(2)：无幽灵回调、新注册镜像未被污染（失配投递丢弃语义不变）
	assert.Len(t, col.snapshot(), 2, "no ghost callback from the stale report (2 precondition active flips only)")
	assert.True(t, m.IsHealthy(host), "the fresh registration's mirror stays unpolluted")
}

// TestReport_ReRegisterStaleFlip_NewGenerationPassiveDetectionAlive 是 R1
// （失配无条件 Reset）的**危害链回归测试**（钉住"被动检测不失活"，对应审计
// P1-3 影响面"被动检测静默失效"）：重注册形态下 stale 报告的翻转残余被 Reset
// 回收后，新世代的真实失败上报必须能正常累计→翻转→投递；且回收后的条目具备
// 完整新鲜语义（窗口过期后的成功上报正常恢复）。
//
// 判别 oracle：无 R1 修复（豁免 Reset）时，条目卡在 passiveHealthy=false——
// 失败路径对已判死 host 永不翻转（timewindow.go），新世代真实失败被静默吞掉
// （IsHealthy 恒 true、无回调），只剩主动探测 fall×interval 兜底 = 危害链复活。
func TestReport_ReRegisterStaleFlip_NewGenerationPassiveDetectionAlive(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	col := &cbCollector{}
	gated := &gatedRealDetector{
		inner:   passive.NewTimeWindowDetector(1, time.Minute, fc), // maxFails=1：stale 单次失败即翻转
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	healthy := &checker.CheckResult{Healthy: true}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(gated), WithRise(1), WithFall(1),
		WithOnStateChange(col.onChange))

	const host = "passive-alive:80"
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{results: []fakeCheckResult{{result: healthy}}})))
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host), "precondition: old registration probed UP")

	// 在途 stale report 阻塞在 Record 门口（lookup 已通过、持有旧 *managedHost）
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		m.ReportFailure(host)
	}()
	select {
	case <-gated.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("gatedRealDetector did not enter Record within 2s")
	}

	// 窗口内：RemoveHost → AddHost 同名重注册 → 新注册探测器立即 UP
	require.NoError(t, m.RemoveHost(host))
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{results: []fakeCheckResult{{result: healthy}}})))
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host), "precondition: fresh registration probed UP")

	// 放行：stale Record 翻转同名条目（passiveHealthy=false）→ cbMu → identity
	// 失配 → R1 无条件 Reset 回收（本断言钉住前置条件成立）
	close(gated.release)
	select {
	case <-reportDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ReportFailure did not return within 2s")
	}
	require.Equal(t, []string{host, host}, gated.resetCallsSnapshot(),
		"precondition: the stale flip must already be reclaimed by the mismatch-path Reset (RemoveHost + report-mismatch)")

	// 危害链判别(1)：新世代真实失败上报正常累计→翻转→投递。
	// 无 R1 修复时：条目残留 passiveHealthy=false → 本次失败永不翻转 → 被静默
	// 吞掉（IsHealthy 恒 true、无回调）= 审计 P1-3"被动检测静默失效"复活
	before := len(col.snapshot())
	m.ReportFailure(host)
	assert.False(t, m.IsHealthy(host),
		"the new generation's real failure must flip passive state (a surviving stale flip stuck at passiveHealthy=false would silently swallow it and leave IsHealthy true)")
	events := col.snapshot()
	require.Len(t, events, before+1,
		"the new generation's passive flip must deliver exactly 1 callback (without the R1 fix the failure is silently swallowed)")
	assert.Equal(t, cbEvent{host: host, healthy: false, result: nil}, events[before],
		"the delivered flip event carries result=nil (R-d)")

	// 危害链判别(2)：回收后条目具备完整新鲜语义——窗口过期后的成功上报正常
	// 恢复翻转（双向钉死"新世代被动检测不失活"）
	fc.Advance(2 * time.Minute) // 越过 failTimeout=1min，惰性恢复可达
	m.ReportSuccess(host)
	assert.True(t, m.IsHealthy(host),
		"the rebuilt entry must recover on success after window expiry (fully fresh semantics)")
	events = col.snapshot()
	require.Len(t, events, before+2, "the recovery flip delivers exactly 1 more callback")
	assert.Equal(t, cbEvent{host: host, healthy: true, result: nil}, events[before+1],
		"the recovery event reports healthy=true with result=nil (R-d)")
}

// ———————————————————————————— 查询 API ————————————————————————————

// TestGetHealthyHosts_Sorted 是 C4 基础层：任意注册序 + 全部健康 →
// 返回按 addr 字典序（sort.Strings），断言用 assert.Equal（顺序敏感，禁 ElementsMatch）。
func TestGetHealthyHosts_Sorted(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	fpd := newFakePassiveDetector()
	m := newTestManager(t, fs, WithClock(fc), WithPassiveDetector(fpd), WithRise(1))

	hosts := []string{"c:80", "a:80", "b:80"} // 非字典序注册
	for _, h := range hosts {
		fchk := &fakeChecker{results: []fakeCheckResult{{result: &checker.CheckResult{Healthy: true}}}}
		require.NoError(t, m.AddHost(h, WithChecker(fchk)))
		m.probeFn(context.Background(), h)
		require.True(t, m.IsHealthy(h))
	}

	assert.Equal(t, []string{"a:80", "b:80", "c:80"}, m.GetHealthyHosts(), "output must be lexicographic (C4, map iteration order forbidden)")
}

// TestHosts_Count 断言 Hosts() 计数随 AddHost/RemoveHost 变化：0→2→1。
func TestHosts_Count(t *testing.T) {
	fs := &fakeScheduler{}
	m := newTestManager(t, fs, WithClock(newFakeClock()), WithPassiveDetector(newFakePassiveDetector()))

	assert.Equal(t, 0, m.Hosts())
	require.NoError(t, m.AddHost("h1:80", WithChecker(&fakeChecker{})))
	require.NoError(t, m.AddHost("h2:80", WithChecker(&fakeChecker{})))
	assert.Equal(t, 2, m.Hosts())
	require.NoError(t, m.RemoveHost("h1:80"))
	assert.Equal(t, 1, m.Hosts())
}

// ———————————————————————————— 分配上界守护（ERC Phase4 R4） ————————————————————————————

// TestZeroAllocGuards_ProbeFnFullCycleAllocsBound 是 C2「FullCycle steady
// allocs==4」的程序化持久守护（spec-review P2-3：既有 wrapper 级守护
// TestTimerQueue_ReusedBoxedHost_ZeroAlloc 与零分配守护均无法捕捉**调用点回退**——
// 若 probeFn 改回直传 string，装箱在 Done/Rearm 调用点复现（+2），或 wrapper 内
// 加回装箱（+1/点），全部门禁将静默通过）。
//
// 上界形态（≤4，**禁止断言 ==4 精确值**）：4 次分配的主体是 context.WithTimeout
// 的 stdlib 内部分配，数量可能随 Go 版本漂移，精确值断言会跨版本误报；装箱回退
// 使实测 ≥5 > 4 必 FAIL，上界形态不影响判别力（变异验证记录见 erc2-repair 报告）。
// setup 与 BenchmarkProbeFn_FullCycle/steady 严格同构：真实 TimerQueue 调度器
// （覆盖 timerqueue.go wrapper 透传路径）+ benchChecker 共享预置结果 +
// interval=1h（Rearm 永不到期、堆大小恒定）+ rise=2 预热两次达 UP 稳态
// （守护段内 riseCount++ 无翻转、无回调路径分配）。
func TestZeroAllocGuards_ProbeFnFullCycleAllocsBound(t *testing.T) {
	m := newBenchManagerTB(t, WithWorkers(1), WithOnStateChange(noopCallback), WithRise(2), WithFall(3))
	t.Cleanup(func() { _ = m.Stop(context.Background()) })
	require.NoError(t, m.AddHost("guard-host", WithChecker(benchChecker{}), WithHostInterval(time.Hour)))
	ctx := context.Background()
	// 预热：rise=2 直调 probeFn 两次达 UP（与 FullCycle steady 预热同形态）
	m.probeFn(ctx, "guard-host")
	m.probeFn(ctx, "guard-host")
	require.True(t, m.IsHealthy("guard-host"), "preheat must bring host UP")

	allocs := testing.AllocsPerRun(1000, func() { m.probeFn(ctx, "guard-host") })
	t.Logf("measured steady-state probeFn allocs/op: %v (upper bound 4)", allocs)
	require.LessOrEqual(t, allocs, float64(4),
		"steady-state probeFn full cycle must stay within the 4-alloc upper bound (C2 boxed-reuse baseline; a boxing regression at the Done/Rearm call sites or inside the scheduler wrappers adds >=+1 alloc and must fail this guard)")
}
