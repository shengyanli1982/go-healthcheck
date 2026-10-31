package passive

// 基准测试套件（passive 包）——设计 SSOT：.agent-work-b652f803/specs/2026-10-29-benchmark-design.md §6.3。
// 覆盖热路径 3 的 detector 段：TimeWindowDetector.Record（leaf lock + map 查找 +
// 窗口计数）与 Reset（RemoveHost 联动路径）。
//
// 时钟纪律（设计 §3.6）：注入 benchClock 固定时间 → 窗口永不过期，steady 路径稳定；
// 不复用带锁 fakeClock（锁成本污染测量）。翻转类路径按段合成原则（§3.5）：
// 本包 FlipCycle 测 detector 判定段，与根包 BenchmarkReport_FlipWithCallback
// （库侧段）之和 ≈ 端到端翻转成本。
//
// 方法学：M3（ResetTimer 唯一计时起点、禁止显式重启计时）；M4（RunParallel =
// 聚合吞吐口径，注释标注）；M5（零分配守护 AllocsPerRun(1000)）。

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// benchFixedTime 是 benchClock 的固定时间常量（窗口永不过期的前提）。
var benchFixedTime = time.Unix(1700000000, 0)

// benchClock 是基准专用固定时间时钟替身：无锁、无 atomic，只读共享合法（M8）。
type benchClock struct{}

// 编译期断言：满足 passive.Clock 接口。
var _ Clock = benchClock{}

// Now 返回固定时间常量。
func (benchClock) Now() time.Time { return benchFixedTime }

// benchHostName 生成基准 host 名（setup 期预生成，防测量循环内 Sprintf 分配）。
func benchHostName(i int) string { return fmt.Sprintf("bench-host-%04d", i) }

// newBenchDetector 构造基准用 TimeWindowDetector（maxFails=3、failTimeout=10s、benchClock）。
func newBenchDetector() *TimeWindowDetector {
	return NewTimeWindowDetector(3, 10*time.Second, benchClock{})
}

// BenchmarkDetector_Record_SuccessSteady 测热路径 3 detector 段：
// leaf lock + map 查找 + count 清零（success 稳态，无翻转 early return）。
// 口径：单线程；allocs/op == 0 硬守护（TestPassiveZeroAllocGuards）。
func BenchmarkDetector_Record_SuccessSteady(b *testing.B) {
	d := newBenchDetector()
	host := benchHostName(0)
	d.Record(host, true) // 预热 1 次：惰性建窗（首建分配不入测量段）

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Record(host, true)
	}
	b.StopTimer()
}

// BenchmarkDetector_Record_FailureAlreadyDown 测判死后失败稳态
// （count++ → count>=maxFails && passiveHealthy==false → 无翻转 return）。
// 口径：单线程；allocs/op == 0 硬守护（TestPassiveZeroAllocGuards）；
// count 随 b.N 增长（int 无溢出风险；固定时钟下窗口永不过期，失败路径永不恢复）。
func BenchmarkDetector_Record_FailureAlreadyDown(b *testing.B) {
	d := newBenchDetector()
	host := benchHostName(0)
	// 预热 3 次失败翻转为被动 DOWN（maxFails=3，不计时段）
	d.Record(host, false)
	d.Record(host, false)
	flipped, newHealthy := d.Record(host, false)
	require.True(b, flipped, "third failure must flip the window")
	require.False(b, newHealthy, "host must be passive-down after the flip")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Record(host, false)
	}
	b.StopTimer()
}

// BenchmarkDetector_Record_FlipCycle 测完整翻转周期（段合成 §3.5：detector 判定段）。
//
// 口径标注：每 iteration = Reset(host) + maxFails 次 failure Record（末次翻转），
// ns/op 为「一次翻转周期（3 Record + 1 Reset）」总成本，单次 Record ≈ ns/op / 4；
// 单线程。翻转计数 == b.N（防消失守卫）；allocs/op 报告不设零断言
// （Reset 的 map delete + 重建窗有分配，基线记录）。
func BenchmarkDetector_Record_FlipCycle(b *testing.B) {
	d := newBenchDetector()
	host := benchHostName(0)
	flips := 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Reset(host) // 清窗重建计数：翻转状态机单向性（失败路径永不恢复）的复现前提
		d.Record(host, false)
		d.Record(host, false)
		flipped, newHealthy := d.Record(host, false) // 第 3 次达 maxFails → 翻转
		if flipped && !newHealthy {
			flips++
		}
	}
	b.StopTimer()
	require.Equal(b, b.N, flips, "every cycle must end with a flip to passive-down")
}

// BenchmarkDetector_Record_Parallel 测 leaf lock（单全局 mutex）争用：
// same-host vs sharded-hosts 差值 = 锁争用成本。
//
// 口径：b.RunParallel = 聚合吞吐语义（M4），与单线程 ns/op 不可横比；
// allocs/op == 0；争用归因 = R3 mutex profile（TimeWindowDetector.mu 主争用帧）。
func BenchmarkDetector_Record_Parallel(b *testing.B) {
	b.Run("same-host", func(b *testing.B) { benchDetectorRecordParallel(b, 1) })
	b.Run("sharded-hosts", func(b *testing.B) { benchDetectorRecordParallel(b, 1000) })
}

// benchDetectorRecordParallel 预热 n 个 host 建窗后 RunParallel 上报 success
// （steady 无翻转路径；n=1 → leaf lock 全争用，n=1000 → 轮转分散）。
func benchDetectorRecordParallel(b *testing.B, n int) {
	b.Helper()
	d := newBenchDetector()
	hosts := make([]string, n)
	for i := 0; i < n; i++ {
		hosts[i] = benchHostName(i)
		d.Record(hosts[i], true) // 预热：惰性建窗（首建分配不入测量段）
	}

	var idx atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d.Record(hosts[int(idx.Add(1)%uint64(n))], true)
		}
	})
	b.StopTimer()
}

// BenchmarkDetector_Reset 测 RemoveHost 联动路径：map delete 不分配；
// 重复 Reset 已无 key 也是生产路径（RemoveHost 幂等）。
// 口径：单线程；allocs/op == 0（TestPassiveZeroAllocGuards）。
func BenchmarkDetector_Reset(b *testing.B) {
	d := newBenchDetector()
	host := benchHostName(0)
	d.Record(host, true) // 预热建窗：首个 Reset 实际删除，其余迭代 delete 未命中（同为生产路径）

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Reset(host)
	}
	b.StopTimer()
}

// TestPassiveZeroAllocGuards 是 passive 包零分配承诺的程序化落点（M5，设计 §6.10）：
// Record success steady / Record failure already-down / Reset / CurrentHealthy == 0；
// AllocsPerRun(1000, f) 硬断言，N=1000 防整除截断假象。
// 注意：host 首次 Record 惰性建窗有分配——守护前必须预热。
// CurrentHealthy 守护覆盖 r4 P1-1 修复引入的复核读取（存在/不存在两种条目形态，
// report 投递前复核路径零分配承诺的 detector 侧落点）。
func TestPassiveZeroAllocGuards(t *testing.T) {
	d := newBenchDetector()

	okHost := benchHostName(0)
	d.Record(okHost, true) // 预热：建窗

	downHost := benchHostName(1)
	d.Record(downHost, false)
	d.Record(downHost, false)
	flipped, newHealthy := d.Record(downHost, false)
	require.True(t, flipped, "third failure must flip the window")
	require.False(t, newHealthy, "host must be passive-down after the flip")

	resetHost := benchHostName(2)
	d.Record(resetHost, true) // 预热建窗：首个被测 Reset 实际删除，后续 delete 未命中（幂等路径）

	missingHost := benchHostName(3) // 守护外预构造：Sprintf 分配不计入守护段（map 查找未命中本身零分配）

	guarded := map[string]func(){
		"Record(success-steady)":       func() { d.Record(okHost, true) },
		"Record(failure-already-down)": func() { d.Record(downHost, false) },
		"Reset":                        func() { d.Reset(resetHost) },
		"CurrentHealthy(existing)":     func() { d.CurrentHealthy(okHost) },
		"CurrentHealthy(missing)":      func() { d.CurrentHealthy(missingHost) },
	}
	for name, f := range guarded {
		allocs := testing.AllocsPerRun(1000, f)
		require.Zero(t, allocs, "zero-alloc guard failed for %s", name)
	}
}
