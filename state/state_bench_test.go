package state

// 基准测试套件（state 包）——设计 SSOT：.agent-work-b652f803/specs/2026-10-29-benchmark-design.md §6.2。
// 覆盖热路径 4：ApplyResult 状态机（rise/fall 计数）、SetPassiveHealthy 镜像写、自锁读。
//
// 共享预置 CheckResult 合法性（设计 §3.1）：ApplyResult 只存指针不改写
// （INV-4 发布后不可变）；Timestamp 预置非零 → 上游 safeCheck 免补填，
// 共享对象跨迭代无写入污染、RunParallel 下无 race。
//
// 方法学：M3（ResetTimer 唯一计时起点、禁止显式重启计时、StopTimer 后 teardown）；
// M4（RunParallel = 聚合吞吐口径，注释标注）；M5（零分配守护 AllocsPerRun(1000)）。

import (
	"errors"
	"testing"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/stretchr/testify/require"
)

// benchFixedTime 是基准预置结果的固定时间戳（非零 → 免上游补填写入，共享安全）。
var benchFixedTime = time.Unix(1700000000, 0)

// benchErrUnhealthy 是 benchBadResult 的事实化错误（M-5 形态）。
var benchErrUnhealthy = errors.New("bench: unhealthy")

// 共享预置探测结果（只读、不可变；ApplyResult 只存指针，共享合法）。
var (
	benchOKResult  = &checker.CheckResult{Healthy: true, Latency: time.Millisecond, Timestamp: benchFixedTime}
	benchBadResult = &checker.CheckResult{Healthy: false, Latency: time.Millisecond, Err: benchErrUnhealthy, Timestamp: benchFixedTime}
)

// BenchmarkApplyResult_SteadySuccess 测热路径 4：UP 稳态计数
// （riseCount++、fallCount=0、lastResult 指针写，无翻转）。
// 口径：单线程；allocs/op == 0 硬守护（TestStateZeroAllocGuards）。
func BenchmarkApplyResult_SteadySuccess(b *testing.B) {
	hs := NewHostState()
	// 预热 rise=2 次达 UP，进入稳态计数段
	hs.ApplyResult(benchOKResult, 2, 3)
	changed, healthy := hs.ApplyResult(benchOKResult, 2, 3)
	require.True(b, changed, "second success must flip host UP")
	require.True(b, healthy, "combined snapshot must be healthy after flip")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		changed, healthy = hs.ApplyResult(benchOKResult, 2, 3)
	}
	b.StopTimer()
	require.False(b, changed, "steady-state success must not flip")
	require.True(b, healthy, "host must stay UP in steady state")
}

// BenchmarkApplyResult_SteadyFailure 测热路径 4：DOWN 稳态计数（fallCount++ 无翻转）。
// 口径：单线程；allocs/op == 0 硬守护（TestStateZeroAllocGuards）。
func BenchmarkApplyResult_SteadyFailure(b *testing.B) {
	hs := NewHostState()
	// 预热：2 次成功达 UP → 3 次失败判死（fall=3），随后进入 DOWN 稳态计数段
	hs.ApplyResult(benchOKResult, 2, 3)
	hs.ApplyResult(benchOKResult, 2, 3)
	require.True(b, hs.IsHealthy(), "preheat must bring host UP")
	hs.ApplyResult(benchBadResult, 2, 3)
	hs.ApplyResult(benchBadResult, 2, 3)
	changed, healthy := hs.ApplyResult(benchBadResult, 2, 3)
	require.True(b, changed, "third failure must flip host DOWN")
	require.False(b, healthy, "combined snapshot must be unhealthy after flip")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		changed, healthy = hs.ApplyResult(benchBadResult, 2, 3)
	}
	b.StopTimer()
	require.False(b, changed, "steady-state failure must not flip again")
	require.False(b, healthy, "host must stay DOWN in steady state")
}

// BenchmarkApplyResult_FlipCycle 测每轮翻转路径（changed=true 返回）：
// rise=1/fall=1，ok/bad result 交替（预置切片 + i%2 选择，无替身状态）。
// 口径：单线程；allocs/op == 0；changed 计数 == b.N（返回值累加防消失）。
func BenchmarkApplyResult_FlipCycle(b *testing.B) {
	hs := NewHostState()
	results := [2]*checker.CheckResult{benchOKResult, benchBadResult}
	flips := 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		changed, _ := hs.ApplyResult(results[i%2], 1, 1)
		if changed {
			flips++
		}
	}
	b.StopTimer()
	require.Equal(b, b.N, flips, "every iteration must flip the active state (rise=1/fall=1)")
}

// BenchmarkSetPassiveHealthy_Flip 测镜像写 + 综合翻转判定：
// 先 ApplyResult 达 activeHealthy=true，循环交替 v=true/false → 每轮综合翻转。
// 口径：单线程；allocs/op == 0（TestStateZeroAllocGuards）。
func BenchmarkSetPassiveHealthy_Flip(b *testing.B) {
	hs := NewHostState()
	preheatChanged, _ := hs.ApplyResult(benchOKResult, 1, 1) // rise=1：一次成功即 UP
	require.True(b, preheatChanged, "preheat must bring host UP")
	flips := 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		changed, _ := hs.SetPassiveHealthy(i%2 == 1) // 交替 false/true，每轮综合翻转
		if changed {
			flips++
		}
	}
	b.StopTimer()
	require.Equal(b, b.N, flips, "every alternating write must flip the combined state")
}

// BenchmarkIsHealthy 测 state 级自锁读（综合 AND 判定）。
// 口径：单线程；allocs/op == 0（TestStateZeroAllocGuards）。
func BenchmarkIsHealthy(b *testing.B) {
	hs := NewHostState()
	hs.ApplyResult(benchOKResult, 2, 3)
	hs.ApplyResult(benchOKResult, 2, 3)
	require.True(b, hs.IsHealthy(), "preheat must bring host UP")

	var healthy bool
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		healthy = hs.IsHealthy()
	}
	b.StopTimer()
	require.True(b, healthy, "host must stay UP")
}

// BenchmarkLastResult 测 state 级自锁读指针（不可变对象 R-d/INV-4）。
// 口径：单线程；allocs/op == 0（TestStateZeroAllocGuards）。
func BenchmarkLastResult(b *testing.B) {
	hs := NewHostState()
	hs.ApplyResult(benchOKResult, 2, 3)

	var res *checker.CheckResult
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res = hs.LastResult()
	}
	b.StopTimer()
	require.Same(b, benchOKResult, res, "LastResult must return the stored pointer")
}

// BenchmarkApplyResult_Parallel_SameHost 测 per-host mutex 争用成本
// （生产场景：探测 worker 与被动翻转并发写同 host）。
//
// 口径：b.RunParallel = 聚合吞吐语义（M4），与单线程 ns/op 不可横比；
// 争用归因 = 与单线程比值 + R3 mutex profile。allocs/op == 0。
func BenchmarkApplyResult_Parallel_SameHost(b *testing.B) {
	hs := NewHostState()
	hs.ApplyResult(benchOKResult, 2, 3)
	hs.ApplyResult(benchOKResult, 2, 3)
	require.True(b, hs.IsHealthy(), "preheat must bring host UP")

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hs.ApplyResult(benchOKResult, 2, 3)
		}
	})
	b.StopTimer()
}

// TestStateZeroAllocGuards 是 state 包零分配承诺的程序化落点（M5，设计 §6.10）：
// ApplyResult(steady) / SetPassiveHealthy / IsHealthy / LastResult == 0；
// AllocsPerRun(1000, f) 硬断言，N=1000 防整除截断假象。
func TestStateZeroAllocGuards(t *testing.T) {
	// ApplyResult steady：预热达 UP，守护段为稳态计数（无翻转）
	hsApply := NewHostState()
	hsApply.ApplyResult(benchOKResult, 2, 3)
	hsApply.ApplyResult(benchOKResult, 2, 3)
	require.True(t, hsApply.IsHealthy(), "preheat must bring host UP")

	// SetPassiveHealthy：activeHealthy=true 是综合翻转前提（rise=1 一次成功即 UP）
	hsPassive := NewHostState()
	hsPassive.ApplyResult(benchOKResult, 1, 1)

	// 读路径：预热一轮使 lastResult 非 nil
	hsRead := NewHostState()
	hsRead.ApplyResult(benchOKResult, 2, 3)

	guarded := map[string]func(){
		"ApplyResult(steady)": func() { hsApply.ApplyResult(benchOKResult, 2, 3) },
		"SetPassiveHealthy":   func() { hsPassive.SetPassiveHealthy(false) },
		"IsHealthy":           func() { hsRead.IsHealthy() },
		"LastResult":          func() { hsRead.LastResult() },
	}
	for name, f := range guarded {
		allocs := testing.AllocsPerRun(1000, f)
		require.Zero(t, allocs, "zero-alloc guard failed for %s", name)
	}
}
