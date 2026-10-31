package passive

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock 是本地测试时钟（任务卡 ST-03 钉死定义，docs/08 §2.1 规格的 Now/Advance 子集）。
// 仅测试使用；自带互斥锁，可在 -race 门禁下被多 goroutine 安全共享（C5：纯 fakeClock，无真实时间）。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1700000000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// TestTimeWindowDetector_InitialState 验证新 host 被动健康初值为 true：
// 首次 Record(true) 不翻转且返回健康（冷启动综合 AND 语义，docs/01）。
func TestTimeWindowDetector_InitialState(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(3, 10*time.Second, clock)

	flipped, newHealthy := d.Record("h", true)
	assert.False(t, flipped, "the first success for a new host must not flip")
	assert.True(t, newHealthy, "passive health must start true")
}

// TestTimeWindowDetector_MaxFailsFlip 验证窗口内累计 maxFails 次失败触发翻转（M2-C5）：
// t0/t0+1s/t0+2s 三次失败，前两次 (false,true)，第三次 (true,false)。
func TestTimeWindowDetector_MaxFailsFlip(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(3, 10*time.Second, clock)

	flipped, newHealthy := d.Record("h", false) // t0，窗口内第 1 次失败
	assert.False(t, flipped)
	assert.True(t, newHealthy)

	clock.Advance(time.Second)
	flipped, newHealthy = d.Record("h", false) // t0+1s，第 2 次失败
	assert.False(t, flipped)
	assert.True(t, newHealthy)

	clock.Advance(time.Second)
	flipped, newHealthy = d.Record("h", false) // t0+2s，第 3 次失败达到 maxFails
	assert.True(t, flipped, "the maxFails-th failure within the window must flip")
	assert.False(t, newHealthy)
}

// TestTimeWindowDetector_MaxFailsMinusOne_NoFlip 验证阈值边界：
// 窗口内仅 maxFails-1（2）次失败不翻转。
func TestTimeWindowDetector_MaxFailsMinusOne_NoFlip(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(3, 10*time.Second, clock)

	flipped, newHealthy := d.Record("h", false) // 第 1 次失败
	assert.False(t, flipped)
	assert.True(t, newHealthy)

	clock.Advance(time.Second)
	flipped, newHealthy = d.Record("h", false) // 第 2 次失败（maxFails-1）
	assert.False(t, flipped, "maxFails-1 failures must not flip")
	assert.True(t, newHealthy)
}

// TestTimeWindowDetector_WindowExpiry_ResetsCount 验证窗口过期重置计数：
// 2 次失败后推进 11s（>failTimeout），第 3 次失败按新窗口 count=1 计，不翻转。
func TestTimeWindowDetector_WindowExpiry_ResetsCount(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(3, 10*time.Second, clock)

	flipped, newHealthy := d.Record("h", false) // 第 1 次失败，开启窗口
	assert.False(t, flipped)
	assert.True(t, newHealthy)

	flipped, newHealthy = d.Record("h", false) // 第 2 次失败（同一时刻）
	assert.False(t, flipped)
	assert.True(t, newHealthy)

	clock.Advance(11 * time.Second) // 窗口过期
	flipped, newHealthy = d.Record("h", false)
	assert.False(t, flipped, "after window expiry the count must reset to 1; below maxFails there must be no flip")
	assert.True(t, newHealthy)
}

// TestTimeWindowDetector_WindowBoundary_ExactlyTimeout 验证过期判定为 `>=` 边界：
// 距 windowStart 恰好等于 failTimeout 即判定过期，第 3 次失败按新窗口 count=1 计。
func TestTimeWindowDetector_WindowBoundary_ExactlyTimeout(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(3, 10*time.Second, clock)

	flipped, newHealthy := d.Record("h", false) // 第 1 次失败，windowStart=t0
	assert.False(t, flipped)
	assert.True(t, newHealthy)

	flipped, newHealthy = d.Record("h", false) // 第 2 次失败（同一时刻）
	assert.False(t, flipped)
	assert.True(t, newHealthy)

	clock.Advance(10 * time.Second) // 恰好 == failTimeout
	flipped, newHealthy = d.Record("h", false)
	assert.False(t, flipped, "exactly at expiry (>=) must be judged expired and reset the count to 1")
	assert.True(t, newHealthy)
}

// TestTimeWindowDetector_SuccessResetsCount 验证成功清零失败计数：
// F,F,S,F,F 无翻转（S 清零后重计至 2）；再 F 才翻转。
func TestTimeWindowDetector_SuccessResetsCount(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(3, 10*time.Second, clock)

	noFlipSteps := []struct {
		success bool
		note    string
	}{
		{success: false, note: "F1"},
		{success: false, note: "F2"},
		{success: true, note: "S clears count"},
		{success: false, note: "F recount 1"},
		{success: false, note: "F recount 2"},
	}
	for _, s := range noFlipSteps {
		clock.Advance(time.Second)
		flipped, newHealthy := d.Record("h", s.success)
		assert.False(t, flipped, "step %s must not flip", s.note)
		assert.True(t, newHealthy, "step %s must stay healthy", s.note)
	}

	clock.Advance(time.Second)
	flipped, newHealthy := d.Record("h", false) // F 重计 3，达 maxFails
	assert.True(t, flipped, "after S clears the count, re-accumulating maxFails failures must flip")
	assert.False(t, newHealthy)
}

// TestTimeWindowDetector_LazyRecovery 验证惰性恢复：
// 翻转后仅推进时间不 Record 则不恢复（无主动恢复/后台 goroutine）；
// 窗口过期后的 Record(true) 触发确定性恢复 (true,true)。
func TestTimeWindowDetector_LazyRecovery(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(3, 10*time.Second, clock)

	for i := 0; i < 2; i++ {
		flipped, newHealthy := d.Record("h", false)
		assert.False(t, flipped)
		assert.True(t, newHealthy)
	}
	flipped, newHealthy := d.Record("h", false) // 第 3 次失败翻转
	assert.True(t, flipped)
	assert.False(t, newHealthy)

	clock.Advance(11 * time.Second) // 窗口已过期，但不调用 Record

	w := d.hosts["h"]
	require.NotNil(t, w, "internal window state must exist after the flip")
	assert.False(t, w.passiveHealthy, "must not recover spontaneously when the window expired without a Record (lazy recovery)")

	flipped, newHealthy = d.Record("h", true)
	assert.True(t, flipped, "a success after window expiry must trigger the recovery flip")
	assert.True(t, newHealthy)
}

// TestTimeWindowDetector_RecoveryBoundary 验证恢复判定为 `>=` 边界：
// 翻转后推进恰好 failTimeout，Record(true) 即恢复。
func TestTimeWindowDetector_RecoveryBoundary(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(3, 10*time.Second, clock)

	// 3 次失败（同一冻结时刻 t0，windowStart=t0）触发翻转
	for i := 0; i < 2; i++ {
		flipped, _ := d.Record("h", false)
		assert.False(t, flipped)
	}
	flipped, newHealthy := d.Record("h", false)
	assert.True(t, flipped)
	assert.False(t, newHealthy)

	clock.Advance(10 * time.Second) // 恰好 == failTimeout
	flipped, newHealthy = d.Record("h", true)
	assert.True(t, flipped, "a success recorded exactly at expiry (>=) must trigger recovery")
	assert.True(t, newHealthy)
}

// TestTimeWindowDetector_FailureNeverRecovers 验证失败路径永不恢复：
// 翻转后窗口过期，Record(false) 仅重开计数窗口（count=1），维持不健康。
func TestTimeWindowDetector_FailureNeverRecovers(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(3, 10*time.Second, clock)

	for i := 0; i < 2; i++ {
		d.Record("h", false)
	}
	flipped, newHealthy := d.Record("h", false)
	assert.True(t, flipped)
	assert.False(t, newHealthy)

	clock.Advance(11 * time.Second) // 窗口已过期
	flipped, newHealthy = d.Record("h", false)
	assert.False(t, flipped, "the failure path must not produce a recovery flip")
	assert.False(t, newHealthy, "the failure path never recovers")
}

// TestTimeWindowDetector_Reset 验证 Reset 清除 host 状态：
// 翻转后 Reset，Record(true) 回到初值 (false,true)；再次翻转需完整 maxFails 次失败。
func TestTimeWindowDetector_Reset(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(3, 10*time.Second, clock)

	for i := 0; i < 2; i++ {
		d.Record("h", false)
	}
	flipped, newHealthy := d.Record("h", false) // 翻转
	assert.True(t, flipped)
	assert.False(t, newHealthy)

	d.Reset("h")

	flipped, newHealthy = d.Record("h", true)
	assert.False(t, flipped, "after Reset the state returns to initial; a success must not flip")
	assert.True(t, newHealthy, "after Reset, passive health must return to the initial true")

	// 再次翻转需要完整 maxFails 次窗口内失败
	flipped, newHealthy = d.Record("h", false)
	assert.False(t, flipped)
	assert.True(t, newHealthy)

	clock.Advance(time.Second)
	flipped, newHealthy = d.Record("h", false)
	assert.False(t, flipped)
	assert.True(t, newHealthy)

	clock.Advance(time.Second)
	flipped, newHealthy = d.Record("h", false)
	assert.True(t, flipped, "after Reset, re-accumulating maxFails failures must flip again")
	assert.False(t, newHealthy)
}

// TestTimeWindowDetector_ResetReclaimsResurrectedEntry 是 P1-3 修复依赖契约的
// 包内观测（根包 report 的 identity 失配路径经 Reset 回收复活孤儿条目）：
// Record 对不存在的 host 惰性创建是 Record 契约（Reset 之后到达的 Record 会
// 复活条目）；Reset 必须能回收**已翻转**（passiveHealthy=false）的复活条目——
// map 条目数经包内直读 d.hosts 观测（导出面无条目计数 API，不扩 API 面积）。
func TestTimeWindowDetector_ResetReclaimsResurrectedEntry(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(1, 10*time.Second, clock)

	// 模拟 P1-3 竞态序列：Reset 先行，随后 Record 到达 → 惰性重建（复活）并翻转
	d.Reset("h")
	flipped, newHealthy := d.Record("h", false)
	assert.True(t, flipped, "maxFails=1: the resurrected entry flips on the first failure")
	assert.False(t, newHealthy)
	assert.Len(t, d.hosts, 1, "Record after Reset resurrects the map entry (lazy creation is the Record contract)")

	// 复活条目处于翻转态（失败路径永不恢复）：后续失败不再翻转
	flipped, _ = d.Record("h", false)
	assert.False(t, flipped, "a resurrected entry stuck at passiveHealthy=false never flips again on failures")

	// Reset 必须回收已翻转的复活条目（无永久泄漏）
	d.Reset("h")
	assert.Empty(t, d.hosts, "Reset must reclaim the resurrected (flipped) entry - no permanent map leak")

	// 回收后再次 Record 从初值重新开始（被动健康 true、无窗口残留）
	flipped, newHealthy = d.Record("h", true)
	assert.False(t, flipped)
	assert.True(t, newHealthy, "after reclaiming, the entry restarts from the initial passive-healthy state")
}

// TestTimeWindowDetector_PerHostIsolation 验证多 host 状态隔离：
// h1 翻转不影响 h2（h2 仍 (false,true)）。
func TestTimeWindowDetector_PerHostIsolation(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(3, 10*time.Second, clock)

	for i := 0; i < 2; i++ {
		d.Record("h1", false)
	}
	flipped, newHealthy := d.Record("h1", false) // h1 翻转
	assert.True(t, flipped)
	assert.False(t, newHealthy)

	flipped, newHealthy = d.Record("h2", false)
	assert.False(t, flipped, "h1's flip must not affect h2")
	assert.True(t, newHealthy, "h2 must keep its independent initial healthy state")

	flipped, newHealthy = d.Record("h2", true)
	assert.False(t, flipped)
	assert.True(t, newHealthy)
}

// TestTimeWindowDetector_ConcurrentRecord 验证并发安全（-race 门禁）：
// maxFails=8，8 个 goroutine 在冻结时钟下各 Record 一次失败，恰好 1 个调用返回 flipped=true。
func TestTimeWindowDetector_ConcurrentRecord(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(8, 10*time.Second, clock)

	const goroutines = 8
	var wg sync.WaitGroup
	flippedSlots := make([]bool, goroutines) // 每 goroutine 独占槽位写入，无需额外同步
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			flipped, _ := d.Record("h", false)
			flippedSlots[i] = flipped // 非测试 goroutine 内只写结果，不使用 require/assert
		}(i)
	}
	wg.Wait()

	flippedCount := 0
	for _, flipped := range flippedSlots {
		if flipped {
			flippedCount++
		}
	}
	assert.Equal(t, 1, flippedCount, "when concurrent accumulation reaches maxFails, exactly 1 call returns flipped=true")
}

// TestTimeWindowDetector_CurrentHealthy 验证可选复核接口 CurrentHealthy 的
// 读取语义（r4 审计 P1-1 修复的 detector 侧契约）：条目不存在时 (true, false)
// ——返回初值语义的 true、ok=false 提示调用方保持自身原值；判死后 (false, true)；
// 窗口过期恢复后 (true, true)；Reset 回收条目后回到不存在语义。
func TestTimeWindowDetector_CurrentHealthy(t *testing.T) {
	clock := newFakeClock()
	d := NewTimeWindowDetector(2, 10*time.Second, clock)

	// 条目不存在：(true, false)——初值语义 + ok=false（调用方保持原值）
	healthy, ok := d.CurrentHealthy("h")
	assert.False(t, ok, "a missing entry must report ok=false so callers keep their own value")
	assert.True(t, healthy, "a missing entry must report the initial passive value true")

	// 判死：窗口内累计 maxFails(2) 次失败翻转为不健康
	d.Record("h", false)
	flipped, _ := d.Record("h", false)
	require.True(t, flipped, "the maxFails-th failure within the window must flip")
	healthy, ok = d.CurrentHealthy("h")
	assert.True(t, ok, "an existing entry must report ok=true")
	assert.False(t, healthy, "a down-scaled host must report passive healthy false")

	// 恢复：窗口过期后的成功上报（惰性恢复触发点）
	clock.Advance(10 * time.Second)
	flipped, _ = d.Record("h", true)
	require.True(t, flipped, "a success after window expiry must recover passive health")
	healthy, ok = d.CurrentHealthy("h")
	assert.True(t, ok, "an existing entry must report ok=true")
	assert.True(t, healthy, "a recovered host must report passive healthy true")

	// Reset 回收条目：回到不存在语义
	d.Reset("h")
	healthy, ok = d.CurrentHealthy("h")
	assert.False(t, ok, "after Reset the entry is gone: ok=false")
	assert.True(t, healthy, "after Reset the entry is gone: the initial passive value true")
}
