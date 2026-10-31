package scheduler

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestScheduler 构造真实 timerQueueScheduler 并注册清理关停。
// 队列可能已在测试体内被显式 Shutdown：重复调用安全（底层 sync.Once
// 幂等关闭 + 已退出的 WaitGroup 等待即刻返回 nil）。
func newTestScheduler(t *testing.T) *timerQueueScheduler {
	t.Helper()
	s, err := New()
	require.NoError(t, err)
	require.NotNil(t, s)
	t.Cleanup(func() {
		_ = s.Shutdown(context.Background())
	})
	return s
}

func TestNew_ReturnsReadyScheduler(t *testing.T) {
	s, err := New()
	require.NoError(t, err)
	require.NotNil(t, s)
	t.Cleanup(func() { _ = s.Shutdown(context.Background()) })

	// 失败路径（类型断言失败）无法对真实 TimerQueue 构造——以代码审查确认：
	// BlockingGetQueue 单断言位于 New()（G3 构造期一次性，R3 收敛），
	// error 消息逐字为 docs/04 §2.2 一条。
	// 此处断言构造期断言结果已正确接线（SF2）。
	assert.NotNil(t, s.tq)
	assert.NotNil(t, s.bq)
}

// TestTimerQueue_SingleWriter_NoOverlap 幂等自测三件套 C-5(1)（M2-C9 单元层）：
// 4 workers 并发消费同一 host，幂等模式保证单写者——enter/exit 严格配对无重叠。
// 只断言次数/配对，不断言精确触发时刻（SF5）。
func TestTimerQueue_SingleWriter_NoOverlap(t *testing.T) {
	s := newTestScheduler(t)

	const interval = 30 * time.Millisecond
	var mu sync.Mutex
	var seq []string

	probeFn := func(host string) {
		mu.Lock()
		seq = append(seq, "enter")
		mu.Unlock()

		time.Sleep(20 * time.Millisecond)

		mu.Lock()
		seq = append(seq, "exit")
		mu.Unlock()

		// 模拟根包收尾（INV-1b）：先 Done 后 Rearm。
		s.Done(host)
		s.Rearm(host, interval)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run(ctx, 4, probeFn)
	s.Schedule("host-a", interval, 0)

	time.Sleep(500 * time.Millisecond)
	s.Cancel("host-a")
	require.NoError(t, s.Shutdown(context.Background()))

	mu.Lock()
	snapshot := append([]string(nil), seq...)
	mu.Unlock()

	// Shutdown 经有界 wg.Wait 等待全部 worker 退出，worker 必先跑完已 pop 的
	// probeFn（"exit" 先于 Done 追加），故此刻序列必然成对闭合。
	require.NotEmpty(t, snapshot)
	assert.Zero(t, len(snapshot)%2, "enter/exit must be strictly paired, seq=%v", snapshot)
	for i, ev := range snapshot {
		if i%2 == 0 {
			assert.Equal(t, "enter", ev, "position %d should be enter (no adjacent enter,enter = single writer without overlap)", i)
		} else {
			assert.Equal(t, "exit", ev, "position %d should be exit", i)
		}
	}
	// 周期 ≈ interval(30ms) + 探测耗时(20ms) = 50ms，500ms 窗口 ≈ 10 次，断言下界。
	assert.GreaterOrEqual(t, len(snapshot)/2, 8, "probe count should be >= 8")
}

// TestTimerQueue_SlowProbe_NoBackToBack 幂等自测三件套 C-5(2)（INV-1a/INV-1b 队列侧证据）：
// 探测耗时(200ms) > interval(50ms) 时，重排必须发生在探测完成之后——
// 相邻探测开始时刻差 ≥200ms，无 Done 后立即重探的背靠背风暴。
func TestTimerQueue_SlowProbe_NoBackToBack(t *testing.T) {
	s := newTestScheduler(t)

	const interval = 50 * time.Millisecond
	var mu sync.Mutex
	var starts []time.Time

	probeFn := func(host string) {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()

		time.Sleep(200 * time.Millisecond) // 探测耗时 > interval

		s.Done(host)
		s.Rearm(host, interval)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run(ctx, 2, probeFn)
	s.Schedule("slow-host", interval, 0)

	time.Sleep(1 * time.Second)
	s.Cancel("slow-host")
	require.NoError(t, s.Shutdown(context.Background()))

	mu.Lock()
	snapshot := append([]time.Time(nil), starts...)
	mu.Unlock()

	require.NotEmpty(t, snapshot)
	for i := 1; i < len(snapshot); i++ {
		diff := snapshot[i].Sub(snapshot[i-1])
		assert.GreaterOrEqual(t, diff, 200*time.Millisecond,
			"start-time gap between probe %d and probe %d should be >= 200ms (no back-to-back storm)", i-1, i)
	}
	// 周期 ≈ 200ms + 50ms = 250ms，1s 窗口 ≈ 4-5 次，断言上界。
	assert.LessOrEqual(t, len(snapshot), 6, "probe count should be <= 6")
}

// TestTimerQueue_DrainWaitsInFlight 幂等自测三件套 C-5(3)（关停完成性，R3 语义）：
// Shutdown 返回时在途探测必已完成——新机制经有界 wg.Wait 保证：worker 跑完
// 已 pop 的 probeFn 才退出，Shutdown 等待全部 worker 退出后才返回 nil。
func TestTimerQueue_DrainWaitsInFlight(t *testing.T) {
	s := newTestScheduler(t)

	started := make(chan struct{})
	finished := make(chan struct{})
	var startOnce, finishOnce sync.Once

	probeFn := func(host string) {
		startOnce.Do(func() { close(started) })
		time.Sleep(150 * time.Millisecond)
		// finished 在 Done 之前关闭：观测点必须先于任何收尾动作，
		// 与 Shutdown 返回（wg.Wait 等 worker 跑完 probeFn 才退出）不构成竞态。
		finishOnce.Do(func() { close(finished) })
		s.Done(host)
		s.Rearm(host, time.Second)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run(ctx, 1, probeFn)
	// offset=0 立即首探；interval=1s 保证测试窗口内无第二次探测。
	s.Schedule("drain-host", time.Second, 0)

	<-started
	require.NoError(t, s.Shutdown(context.Background()))

	// Shutdown 返回时在途 probeFn 的探测工作必然已完成（wg.Wait 等 worker 退出）。
	select {
	case <-finished:
	default:
		t.Fatal("in-flight probe not finished when Shutdown returned (finished not closed)")
	}
}

// TestTimerQueue_DrainTimeout 超时路径（R3 语义）：在途探测(300ms)阻塞超过
// ctx(50ms)，Shutdown 返回 ctx error（队列已被立即关闭、不再等待 worker，
// Stop 报错由消费者处置）。
func TestTimerQueue_DrainTimeout(t *testing.T) {
	s := newTestScheduler(t)

	started := make(chan struct{})
	var startOnce sync.Once

	probeFn := func(host string) {
		startOnce.Do(func() { close(started) })
		time.Sleep(300 * time.Millisecond)
		s.Done(host)
		s.Rearm(host, time.Second)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run(ctx, 1, probeFn)
	s.Schedule("timeout-host", time.Second, 0)

	<-started
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer shutdownCancel()
	assert.ErrorIs(t, s.Shutdown(shutdownCtx), context.DeadlineExceeded,
		"should return the ctx error when an in-flight probe blocks beyond the ctx")

	// Cleanup：让在途 probeFn 自然跑完、worker 经已关闭队列的 GetWithContext 出错退出，
	// 避免残留 goroutine 干扰后续泄漏测试（TestScheduler_WorkerExit_NoLeak）。
	time.Sleep(400 * time.Millisecond)
}

// TestTimerQueue_ShutdownAfterCancel_NoSpuriousTimeout P1 关停竞态回归钉住
// （docs/04 R3 / ADR-001 决策 #33）：复刻根包 Stop 的 cancel-first 顺序——
// 先 cancel(ctx) 再 Shutdown。旧机制（ShutdownWithDrain，R3 已改为 Shutdown + 有界 wg.Wait）下 worker 经
// GetWithContext 的 select 随机走 ctx.Done 弃单退出，而 scheduler 协程 carry
// 到期项绕过 draining 拒绝直入内层 list，滞留项无 worker 消费 → drain 判定
// 不可达 → 必然卡满 ctx 超时后返回 spurious "context deadline exceeded"。
// 新机制（Shutdown 立即关闭 + 有界 wg.Wait）下队列即刻关闭、worker 经
// ErrQueueIsClosed/ctx.Done 退出，Shutdown 必须快速返回 nil。
func TestTimerQueue_ShutdownAfterCancel_NoSpuriousTimeout(t *testing.T) {
	s := newTestScheduler(t)

	const interval = 5 * time.Millisecond
	probeFn := func(host string) {
		time.Sleep(1 * time.Millisecond)
		s.Done(host)
		s.Rearm(host, interval)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run(ctx, 2, probeFn)
	s.Schedule("race-host", interval, 0)

	time.Sleep(100 * time.Millisecond) // 跑若干探测周期，host 处于 堆中/在队/在途 任一相

	// 根包 Stop 的真实顺序：cancel-first，再 Shutdown（携带充裕超时）。
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	start := time.Now()
	err := s.Shutdown(shutdownCtx)
	elapsed := time.Since(start)

	require.NoError(t, err, "after cancel-first, Shutdown must not return a spurious timeout error")
	assert.Less(t, elapsed, 1*time.Second, "Shutdown should return quickly (must not burn the full ctx timeout)")
}

// TestScheduler_Cancel_NotScheduled SF3：对未调度 host 调 Cancel 不 panic——
// 底层返回 false 是正常路径（未入堆），接口无返回值不透出。
func TestScheduler_Cancel_NotScheduled(t *testing.T) {
	s := newTestScheduler(t)
	require.NotPanics(t, func() { s.Cancel("ghost") })
}

// TestScheduler_ScheduleAfterShutdown_Benign 关停竞态良性路径：
// Shutdown 完成后 Schedule → 底层 PutAfter 返回 ErrQueueIsClosed 被忽略，不 panic。
func TestScheduler_ScheduleAfterShutdown_Benign(t *testing.T) {
	s := newTestScheduler(t)
	require.NoError(t, s.Shutdown(context.Background()))
	require.NotPanics(t, func() { s.Schedule("late-host", 10*time.Millisecond, 0) })
}

// TestScheduler_WorkerExit_NoLeak M2-C8 协同：Shutdown 成功返回后
// 全部 worker 与底层 timer 协程退出，goroutine 计数回落至基线。
func TestScheduler_WorkerExit_NoLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()

	s := newTestScheduler(t)
	ctx, cancel := context.WithCancel(context.Background())
	s.Run(ctx, 2, func(host string) {
		s.Done(host)
		s.Rearm(host, time.Second)
	})
	cancel()
	require.NoError(t, s.Shutdown(context.Background()))

	deadline := time.Now().Add(2 * time.Second)
	current := runtime.NumGoroutine()
	for time.Now().Before(deadline) && current > baseline {
		time.Sleep(20 * time.Millisecond)
		current = runtime.NumGoroutine()
	}
	assert.LessOrEqual(t, current, baseline, "goroutine count should fall back to baseline (no leak)")
}

// TestScheduler_RunMultipleHosts 多 host 均被消费：3 host × 2 workers，
// 300ms 窗口内每个 host 探测次数均 ≥3。
func TestScheduler_RunMultipleHosts(t *testing.T) {
	s := newTestScheduler(t)

	const interval = 20 * time.Millisecond
	hosts := []string{"host-1", "host-2", "host-3"}

	var mu sync.Mutex
	counts := make(map[string]int, len(hosts))

	probeFn := func(host string) {
		mu.Lock()
		counts[host]++
		mu.Unlock()
		s.Done(host)
		s.Rearm(host, interval)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run(ctx, 2, probeFn)
	for _, h := range hosts {
		s.Schedule(h, interval, 0)
	}

	time.Sleep(300 * time.Millisecond)
	for _, h := range hosts {
		s.Cancel(h)
	}
	require.NoError(t, s.Shutdown(context.Background()))

	mu.Lock()
	snapshot := make(map[string]int, len(counts))
	for k, v := range counts {
		snapshot[k] = v
	}
	mu.Unlock()

	for _, h := range hosts {
		assert.GreaterOrEqual(t, snapshot[h], 3, "%s probe count should be >= 3 (all hosts consumed)", h)
	}
}

// TestTimerQueue_ReusedBoxedHost_ZeroAlloc C2 守护：wrapper 方法（Schedule/Cancel/
// Done/Rearm）以 any 透传调用方预装箱值，自身零分配——string→any 装箱由调用方
// 一次完成并复用（根包 managedHost.boxed 形态），消除热路径每调用 16B 装箱
// （pprof 报告 §4(4)：timerqueue.go PutAfter/Cancel 调用点各 1.0 alloc/调用）。
// 变量形态装箱（var h any = 变量 string）防常量字符串编译期静态装箱伪影。
func TestTimerQueue_ReusedBoxedHost_ZeroAlloc(t *testing.T) {
	s := newTestScheduler(t)

	hostStr := "alloc-host" // 变量形态：string→any 转换无法被编译期静态化
	var host any = hostStr  // 调用方一次装箱，此后全路径复用（C2 契约）

	// 预热：elementpool 建池 + 堆首节点就位（首建分配不入守护段）
	s.Schedule(host, time.Hour, time.Hour)
	s.Cancel(host)
	s.Done(host)
	s.Rearm(host, time.Hour)

	doneRearm := testing.AllocsPerRun(100, func() {
		s.Done(host)             // 非 processing 值 → 安全 no-op（timerqueue Done 契约）
		s.Rearm(host, time.Hour) // PutAfter(1h) 永不到期；同值替换 → 堆恒定
	})
	require.Zero(t, doneRearm, "Done/Rearm with a reused boxed host must be zero-alloc (wrapper must not box)")

	schedCancel := testing.AllocsPerRun(100, func() {
		s.Schedule(host, time.Hour, time.Hour) // PutAfter(1h)：入堆（同值替换，堆至多 1 元素）
		s.Cancel(host)                         // 堆删除（底层 false 是正常路径 SF3）
	})
	require.Zero(t, schedCancel, "Schedule/Cancel with a reused boxed host must be zero-alloc (wrapper must not box)")

	require.NoError(t, s.Shutdown(context.Background()), "Shutdown must return nil (no worker leak)")
}
