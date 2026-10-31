package healthcheck

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/shengyanli1982/go-healthcheck/passive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// afterRecordGateDetector 是包装真实 TimeWindowDetector 的确定性编排替身
// （r4 审计 P1-1「stale 快照覆盖」窗口编排专用；与 gatedRealDetector /
// gatedPassiveDetector 非重复定义——两者的 gate 都在 Record 生效之前，只能
// 钉住「Record 未执行」的窗口；本缺陷的交错窗口在 Record 判定**之后**、
// cbMu 获取**之前**（「晚判定早投递」），需要 gate 位于 inner.Record 完成后、
// 包装层返回前）：第一个完成 inner.Record 的调用者（CAS 抢占，恰一个）在
// 返回前 close(recordDone)（判定已落地 detector、调用方尚未取 cbMu），
// 随后阻塞至测试侧 close(release) 才把 (flipped, newHealthy) 返回给 report——
// 把交错钉死在「G1 判死翻转已判定、投递挂起；G2 恢复翻转完整先行投递」点，
// 无真实时序依赖，-race 稳定。门控期间后续 Record 直通（CAS 已被抢占，
// 不同于 sync.Once——Once 的闭包未返回前会把后续调用者一并挡在 Do 内）。
// Reset 直通 inner（编排场景无 RemoveHost 干扰，无需计数）。
type afterRecordGateDetector struct {
	inner      *passive.TimeWindowDetector
	recordDone chan struct{} // 第一个 Record 的 inner 判定已落地信号（close 一次）
	release    chan struct{} // 测试侧注入：close 放行门控 Record 的返回
	gated      atomic.Bool   // CAS 抢占：恰一个 Record 调用进入门控分支
}

// 编译期断言：满足 PassiveDetector 接口。
var _ passive.PassiveDetector = (*afterRecordGateDetector)(nil)

// Record 先委托真实 detector 完成判定，第一个调用者在返回前门控阻塞。
func (d *afterRecordGateDetector) Record(host string, success bool) (bool, bool) {
	flipped, newHealthy := d.inner.Record(host, success)
	if d.gated.CompareAndSwap(false, true) {
		close(d.recordDone) // 判定已落地、投递未发生
		<-d.release         // 钉住「Record 返回前 = 取 cbMu 前」
	}
	return flipped, newHealthy
}

// CurrentHealthy 直通真实 detector（修复的复核路径必须命中真实判定值，
// 替身若不实现可选接口会使 report 的类型断言失败、退回旧路径、测试虚假失败）。
func (d *afterRecordGateDetector) CurrentHealthy(host string) (bool, bool) {
	return d.inner.CurrentHealthy(host)
}

// Reset 直通真实 detector。
func (d *afterRecordGateDetector) Reset(host string) { d.inner.Reset(host) }

// TestReport_SlowCallbackStaleOverwrite_MirrorConvergesToDetector 是 r4 审计
// P1-1 修复的回归测试（钉死「投递前复核」语义）：Record 判定与
// SetPassiveHealthy 投递两阶段仅隔 cbMu——慢回调使 G1（判死翻转，newHealthy=false）
// 挂起在取 cbMu 之前，G2（恢复翻转，newHealthy=true）插队先获 cbMu 完成投递后，
// G1 若以 stale false 直接覆盖镜像，即产生 detector(true)/镜像(false) 永久分歧
// （持续成功流量下 Record 恒 !flipped 早退，全库再无写镜像路径）。
//
// 判别 oracle：交错完成后断言 IsHealthy(host)==true。修复前 G1 的 stale 覆盖
// 使 IsHealthy 恒 false（2000 迭代独立复现 6.4% 分歧率的确定性平移）；
// 修复后 report 在 identity 复核通过后、投递之前对 detector 复核当前值，
// stale 快照被替换、镜像终态收敛到 detector 当前值。
// 全程 channel 同步构造确定性交错，无 sleep。
func TestReport_SlowCallbackStaleOverwrite_MirrorConvergesToDetector(t *testing.T) {
	fc := newFakeClock()
	fs := &fakeScheduler{}
	col := &cbCollector{}
	gated := &afterRecordGateDetector{
		inner:      passive.NewTimeWindowDetector(1, time.Minute, fc), // maxFails=1：单次失败即判死翻转
		recordDone: make(chan struct{}),
		release:    make(chan struct{}),
	}
	healthy := &checker.CheckResult{Healthy: true}
	m := newTestManager(t, fs,
		WithClock(fc), WithPassiveDetector(gated), WithRise(1), WithFall(1),
		WithOnStateChange(col.onChange))

	const host = "stale-overwrite:80"
	require.NoError(t, m.AddHost(host, WithChecker(&fakeChecker{results: []fakeCheckResult{{result: healthy}}})))
	m.probeFn(context.Background(), host)
	require.True(t, m.IsHealthy(host), "precondition: host probed UP")
	require.Len(t, col.snapshot(), 1, "precondition: exactly one active flip event")

	// G1（判死翻转）：inner Record 完成判定（detector passiveHealthy=false、
	// windowStart=now），挂起在取 cbMu 之前（recordDone 已发、release 未开）。
	g1Done := make(chan struct{})
	go func() {
		defer close(g1Done)
		m.ReportFailure(host)
	}()
	select {
	case <-gated.recordDone:
	case <-time.After(2 * time.Second):
		t.Fatal("afterRecordGateDetector did not finish its first inner Record within 2s")
	}

	// 推进时钟越过 failTimeout：G2 的成功上报才满足惰性恢复的窗口过期条件
	// （判死窗口起点 = G1 的 Record 时刻）。
	fc.Advance(2 * time.Minute)

	// G2（恢复翻转）：cbMu 此刻无人持有（G1 挂在 Record 里、未取锁），G2 完整
	// 走完 复核-投递链（detector passiveHealthy=true）。注意 G2 的
	// SetPassiveHealthy(true) 对镜像无变化（镜像从未写过 false）→ changed=false
	// → 不投递事件（语义自洽：镜像从未离开过 true）。
	g2Done := make(chan struct{})
	go func() {
		defer close(g2Done)
		m.ReportSuccess(host)
	}()
	select {
	case <-g2Done:
	case <-time.After(2 * time.Second):
		t.Fatal("G2 ReportSuccess did not return within 2s (it must acquire cbMu unimpeded)")
	}

	// 释放 G1：其 Record 快照 (flipped=true, newHealthy=false) 已 stale——
	// 修复后 report 以 detector 当前值（true）替换 stale 快照再投递，
	// 镜像收敛；修复前直接以 stale false 覆盖 → 永久分歧。
	close(gated.release)
	select {
	case <-g1Done:
	case <-time.After(2 * time.Second):
		t.Fatal("G1 ReportFailure did not return within 2s")
	}

	// 核心判别（修复前 FAIL）：镜像必须收敛到 detector 当前值。
	assert.True(t, m.IsHealthy(host),
		"the mirror must converge to the detector's current value (true): a stale false overwrite leaves detector(true)/mirror(false) permanently divergent (steady success traffic keeps Record !flipped, no mirror-write path remains)")

	// 事件数判别（修复前 FAIL）：仅前置主动 UP 1 次；G2 复核/投递无变化不投递，
	// G1 复核替换后 newHealthy=true 与镜像现状一致（changed=false）同样不投递。
	// 修复前 G1 以 stale false 覆盖触发综合翻转，投递第 2 个事件（healthy=false）。
	events := col.snapshot()
	require.Len(t, events, 1,
		"the stale report must deliver no flip event after the recheck (only the precondition active flip remains)")
	// 唯一事件为前置主动翻转：host/healthy 字段断言（result 携带探测结果、非 nil，
	// 主动翻转语义；无被动翻转事件存在，result=nil 断言无从落点）。
	assert.Equal(t, host, events[0].host, "the single event must belong to the test host")
	assert.True(t, events[0].healthy, "the single event must be the precondition active flip (healthy=true)")
}
