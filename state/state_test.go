package state

import (
	"sync"
	"testing"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHostState_ColdStart 验证冷启动语义（M2-C2，docs/03 §1.2）：
// activeHealthy 初始 false（DOWN，nginx default_down 对齐）、passiveHealthy
// 初始 true（无流量不误判），综合 IsHealthy()==false（确定性 AND）。
func TestHostState_ColdStart(t *testing.T) {
	s := NewHostState()
	require.NotNil(t, s, "NewHostState must return non-nil")

	assert.False(t, s.activeHealthy, "C2: activeHealthy must start false (DOWN)")
	assert.True(t, s.passiveHealthy, "passiveHealthy must start true")
	assert.Zero(t, s.riseCount, "riseCount must start at 0")
	assert.Zero(t, s.fallCount, "fallCount must start at 0")
	assert.False(t, s.IsHealthy(), "C2: cold-start combined health must be false (AND semantics)")
	assert.Nil(t, s.LastResult(), "LastResult must be nil before any probe")
}

// transitionStep 描述转换表驱动用例中单次 ApplyResult 的输入与全部期望输出。
type transitionStep struct {
	probeHealthy bool // 本次探测结果 CheckResult.Healthy
	wantChanged  bool // 期望返回的 changed（ActiveHealthy 翻转）
	wantHealthy  bool // 期望返回的 healthy（锁内综合快照）
	wantActive   bool // 期望 activeHealthy 字段
	wantRise     int  // 期望 riseCount 字段
	wantFall     int  // 期望 fallCount 字段
}

// TestApplyResult_TransitionTable 覆盖 rise/fall 状态转换表全部格子
// （M2-C1，docs/03 §1.3 逐格 + 反向重置 + 阈值边界；默认 rise=2/fall=3，
// 边界用例注明）。表驱动纯事件序列喂入，不需要调度器（docs/08 §3.1）。
func TestApplyResult_TransitionTable(t *testing.T) {
	tests := []struct {
		name  string
		rise  int
		fall  int
		setup func(s *HostState) // 前置状态（同包直写字段）
		steps []transitionStep
	}{
		{
			// 表格行 1（DOWN+Success）未达阈值：计数但翻转条件不满足
			name: "(1) DOWN+S once: count, no flip",
			rise: 2, fall: 3,
			steps: []transitionStep{
				{probeHealthy: true, wantChanged: false, wantHealthy: false, wantActive: false, wantRise: 1, wantFall: 0},
			},
		},
		{
			// 表格行 1（DOWN+Success）达阈值：第 rise 次翻转 UP
			name: "(2) DOWN+S rise times in a row: flip UP at rise-th",
			rise: 2, fall: 3,
			steps: []transitionStep{
				{probeHealthy: true, wantChanged: false, wantHealthy: false, wantActive: false, wantRise: 1, wantFall: 0},
				{probeHealthy: true, wantChanged: true, wantHealthy: true, wantActive: true, wantRise: 2, wantFall: 0},
			},
		},
		{
			// 表格行 2（DOWN+Failure）：计数但已经 DOWN 不再翻转（越过阈值也不翻转）
			name: "(3) DOWN+F repeated: count, no flip",
			rise: 2, fall: 3,
			steps: []transitionStep{
				{probeHealthy: false, wantChanged: false, wantHealthy: false, wantActive: false, wantRise: 0, wantFall: 1},
				{probeHealthy: false, wantChanged: false, wantHealthy: false, wantActive: false, wantRise: 0, wantFall: 2},
				{probeHealthy: false, wantChanged: false, wantHealthy: false, wantActive: false, wantRise: 0, wantFall: 3},
			},
		},
		{
			// 表格行 3（UP+Success）：计数但已经 UP 不再翻转（越过阈值也不翻转）
			name: "(4) UP+S repeated: count, no flip",
			rise: 2, fall: 3,
			setup: func(s *HostState) { s.activeHealthy = true },
			steps: []transitionStep{
				{probeHealthy: true, wantChanged: false, wantHealthy: true, wantActive: true, wantRise: 1, wantFall: 0},
				{probeHealthy: true, wantChanged: false, wantHealthy: true, wantActive: true, wantRise: 2, wantFall: 0},
				{probeHealthy: true, wantChanged: false, wantHealthy: true, wantActive: true, wantRise: 3, wantFall: 0},
			},
		},
		{
			// 表格行 4（UP+Failure）达阈值：第 fall 次翻转 DOWN
			name: "(5) UP+F fall times in a row: flip DOWN at fall-th",
			rise: 2, fall: 3,
			setup: func(s *HostState) { s.activeHealthy = true },
			steps: []transitionStep{
				{probeHealthy: false, wantChanged: false, wantHealthy: true, wantActive: true, wantRise: 0, wantFall: 1},
				{probeHealthy: false, wantChanged: false, wantHealthy: true, wantActive: true, wantRise: 0, wantFall: 2},
				{probeHealthy: false, wantChanged: true, wantHealthy: false, wantActive: false, wantRise: 0, wantFall: 3},
			},
		},
		{
			// 反向重置（nginx 语义，docs/03 §1.3 关键特征）：DOWN 态 S×1 后 F
			name: "(6) DOWN reverse reset: F after S clears riseCount",
			rise: 2, fall: 3,
			steps: []transitionStep{
				{probeHealthy: true, wantChanged: false, wantHealthy: false, wantActive: false, wantRise: 1, wantFall: 0},
				{probeHealthy: false, wantChanged: false, wantHealthy: false, wantActive: false, wantRise: 0, wantFall: 1},
			},
		},
		{
			// 反向重置（nginx 语义）：UP 态 F×2 后 S
			name: "(6) UP reverse reset: S after two F clears fallCount",
			rise: 2, fall: 3,
			setup: func(s *HostState) { s.activeHealthy = true },
			steps: []transitionStep{
				{probeHealthy: false, wantChanged: false, wantHealthy: true, wantActive: true, wantRise: 0, wantFall: 1},
				{probeHealthy: false, wantChanged: false, wantHealthy: true, wantActive: true, wantRise: 0, wantFall: 2},
				{probeHealthy: true, wantChanged: false, wantHealthy: true, wantActive: true, wantRise: 1, wantFall: 0},
			},
		},
		{
			// 阈值边界：rise=1 单次成功即翻转 UP
			name: "(7) boundary rise=1: single success flips",
			rise: 1, fall: 3,
			steps: []transitionStep{
				{probeHealthy: true, wantChanged: true, wantHealthy: true, wantActive: true, wantRise: 1, wantFall: 0},
			},
		},
		{
			// 阈值边界：fall=1 单次失败即翻转 DOWN
			name: "(7) boundary fall=1: single failure flips",
			rise: 2, fall: 1,
			setup: func(s *HostState) { s.activeHealthy = true },
			steps: []transitionStep{
				{probeHealthy: false, wantChanged: true, wantHealthy: false, wantActive: false, wantRise: 0, wantFall: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewHostState()
			require.NotNil(t, s)
			if tt.setup != nil {
				tt.setup(s)
			}
			for i, step := range tt.steps {
				result := &checker.CheckResult{Healthy: step.probeHealthy}
				changed, healthy := s.ApplyResult(result, tt.rise, tt.fall)
				assert.Equal(t, step.wantChanged, changed,
					"step %d: changed must equal whether ActiveHealthy flipped", i)
				assert.Equal(t, step.wantHealthy, healthy,
					"step %d: healthy must equal the combined snapshot under lock (passiveHealthy always true)", i)
				assert.Equal(t, step.wantActive, s.activeHealthy, "step %d: activeHealthy", i)
				assert.Equal(t, step.wantRise, s.riseCount, "step %d: riseCount", i)
				assert.Equal(t, step.wantFall, s.fallCount, "step %d: fallCount", i)
			}
		})
	}
}

// TestApplyResult_ChangedSemantics_ActiveFlip 钉死 ApplyResult 的 changed
// 语义 = ActiveHealthy 翻转（非综合翻转）：passiveHealthy=false 时 active
// false→true 翻转，changed==true 但 healthy==false（两者语义不同，分别断言）。
func TestApplyResult_ChangedSemantics_ActiveFlip(t *testing.T) {
	s := NewHostState()
	require.NotNil(t, s)
	s.passiveHealthy = false // 同包直写：模拟被动检测已摘除

	// rise=2：第 1 次成功未达阈值，active 不翻转
	changed, healthy := s.ApplyResult(&checker.CheckResult{Healthy: true}, 2, 3)
	assert.False(t, changed, "must not flip below the rise threshold")
	assert.False(t, healthy)

	// 第 2 次成功：active false→true 翻转
	changed, healthy = s.ApplyResult(&checker.CheckResult{Healthy: true}, 2, 3)
	assert.True(t, changed, "changed=ActiveHealthy flip semantics: active false->true must yield true")
	assert.False(t, healthy, "healthy=combined snapshot (active&&passive): must be false when passive=false")
	assert.True(t, s.activeHealthy, "activeHealthy must have flipped to true")
	assert.False(t, s.passiveHealthy, "passiveHealthy must not be rewritten by the active path")
}

// TestApplyResult_LastResult 验证 INV-4：每轮传入新建 *CheckResult，
// LastResult() 指针恒等于最近一次传入指针；旧结果被替换；初始 nil。
func TestApplyResult_LastResult(t *testing.T) {
	s := NewHostState()
	require.NotNil(t, s)
	assert.Nil(t, s.LastResult(), "must be nil initially, before any probe")

	r1 := &checker.CheckResult{Healthy: true, StatusCode: 200}
	s.ApplyResult(r1, 1, 3)
	assert.Same(t, r1, s.LastResult(), "INV-4: LastResult must be the exact pointer most recently passed in")

	r2 := &checker.CheckResult{Healthy: false, StatusCode: 503}
	s.ApplyResult(r2, 1, 3)
	assert.Same(t, r2, s.LastResult(), "INV-4: a new result must replace the old result pointer")
	assert.NotSame(t, r1, s.LastResult(), "the old pointer must not be returned again")
}

// TestSetPassiveHealthy_CombinedFlipSemantics 钉死 SetPassiveHealthy 的
// changed 语义 = 综合健康值（active AND passive）翻转（docs/01 §4.2），
// 与 ApplyResult.changed（主动翻转）语义不同。四态全格 + 重复写相同值。
func TestSetPassiveHealthy_CombinedFlipSemantics(t *testing.T) {
	tests := []struct {
		name        string
		active      bool // 前置 activeHealthy（同包直写）
		mirrorFrom  bool // 前置 passiveHealthy 镜像值
		setTo       bool // SetPassiveHealthy 入参
		wantChanged bool
		wantHealthy bool
	}{
		{name: "(1) active=false mirror true->false: combined stays false, no flip", active: false, mirrorFrom: true, setTo: false, wantChanged: false, wantHealthy: false},
		{name: "(2) active=false mirror false->true: combined stays false, no flip", active: false, mirrorFrom: false, setTo: true, wantChanged: false, wantHealthy: false},
		{name: "(3) active=true mirror true->false: combined flips true->false", active: true, mirrorFrom: true, setTo: false, wantChanged: true, wantHealthy: false},
		{name: "(4) active=true mirror false->true: combined flips false->true", active: true, mirrorFrom: false, setTo: true, wantChanged: true, wantHealthy: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewHostState()
			require.NotNil(t, s)
			s.activeHealthy = tt.active
			s.passiveHealthy = tt.mirrorFrom

			changed, healthy := s.SetPassiveHealthy(tt.setTo)
			assert.Equal(t, tt.wantChanged, changed, "changed must equal whether the combined health (active&&passive) flipped")
			assert.Equal(t, tt.wantHealthy, healthy, "healthy must equal the combined snapshot under lock")
			assert.Equal(t, tt.setTo, s.passiveHealthy, "the mirrored bool must be written")
			assert.Equal(t, tt.active, s.activeHealthy, "the passive path must not rewrite activeHealthy")
		})
	}

	t.Run("(5) rewriting same value does not flip", func(t *testing.T) {
		s := NewHostState()
		require.NotNil(t, s)
		s.activeHealthy = true // passiveHealthy 初始 true

		changed, healthy := s.SetPassiveHealthy(true)
		assert.False(t, changed, "writing the same value (true->true) leaves the combination unchanged; changed must be false")
		assert.True(t, healthy)

		changed, healthy = s.SetPassiveHealthy(false)
		assert.True(t, changed, "true->false with active=true flips the combination")
		assert.False(t, healthy)

		changed, healthy = s.SetPassiveHealthy(false)
		assert.False(t, changed, "writing the same value (false->false) leaves the combination unchanged; changed must be false")
		assert.False(t, healthy)
	})
}

// TestIsHealthy_ANDTruthTable 验证综合判定的确定性 AND 真值表
// （M2-C6，docs/03 §1.1）：仅 (active=true, passive=true) → true。
func TestIsHealthy_ANDTruthTable(t *testing.T) {
	tests := []struct {
		active  bool
		passive bool
		want    bool
	}{
		{active: false, passive: false, want: false},
		{active: false, passive: true, want: false},
		{active: true, passive: false, want: false},
		{active: true, passive: true, want: true},
	}
	for _, tt := range tests {
		s := NewHostState()
		require.NotNil(t, s)
		s.activeHealthy = tt.active
		s.passiveHealthy = tt.passive
		assert.Equal(t, tt.want, s.IsHealthy(),
			"AND truth table: IsHealthy(active=%v, passive=%v)", tt.active, tt.passive)
	}
}

// determinismSnapshot 是 C3 确定性断言的状态快照（任务卡钉死 4 字段）。
type determinismSnapshot struct {
	activeHealthy  bool
	passiveHealthy bool
	riseCount      int
	fallCount      int
}

// TestHostState_Determinism_100Runs 验证状态机纯函数确定性（M2-C3 单元层，
// docs/03 机制 5）：固定事件序列 [S,S,F,S,F,F,S]（rise=2/fall=3，禁用
// 随机数，§0.2）喂入新建 HostState，重复 100 次，全部快照与第 1 次
// 逐字段相同；第 1 次快照同时与手工推导期望值核对。
func TestHostState_Determinism_100Runs(t *testing.T) {
	events := []bool{true, true, false, true, false, false, true}
	const runs = 100

	// 手工推导（docs/03 §1.3 转换表）：S→rise1; S→rise2 翻转 UP;
	// F→fall1; S→rise1(清 fall); F→fall1; F→fall2; S→rise1(清 fall)。
	wantFirst := determinismSnapshot{activeHealthy: true, passiveHealthy: true, riseCount: 1, fallCount: 0}

	var first determinismSnapshot
	for i := 0; i < runs; i++ {
		s := NewHostState()
		require.NotNil(t, s)
		for _, healthy := range events {
			s.ApplyResult(&checker.CheckResult{Healthy: healthy}, 2, 3)
		}
		snap := determinismSnapshot{
			activeHealthy:  s.activeHealthy,
			passiveHealthy: s.passiveHealthy,
			riseCount:      s.riseCount,
			fallCount:      s.fallCount,
		}
		if i == 0 {
			first = snap
			assert.Equal(t, wantFirst, first, "the 1st run snapshot must equal the hand-derived expectation")
			continue
		}
		assert.Equal(t, first, snap, "C3: run %d snapshot must match the 1st run field by field", i+1)
	}
}

// TestApplyResult_ConcurrentReadWrite 验证并发安全（docs/08 §6 -race 门禁）：
// 1 写者循环 ApplyResult + 4 读者循环 IsHealthy()/LastResult()，-race 零报告。
// goroutine 内不使用断言库（race detector 即 oracle；§0.2 require 禁入非测试 goroutine）。
func TestApplyResult_ConcurrentReadWrite(t *testing.T) {
	s := NewHostState()
	require.NotNil(t, s)

	const writes = 200
	const readers = 4
	const readsPerReader = 200

	var wg sync.WaitGroup
	wg.Add(1 + readers)
	go func() {
		defer wg.Done()
		for i := 0; i < writes; i++ {
			s.ApplyResult(&checker.CheckResult{Healthy: i%2 == 0}, 2, 3)
		}
	}()
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < readsPerReader; i++ {
				_ = s.IsHealthy()
				_ = s.LastResult()
			}
		}()
	}
	wg.Wait()

	// 交替 S/F 序列（rise=2 永不满足连续 2 次成功）：active 恒 false；
	// 最后一次写入为 F（i=199 奇数）→ fallCount=1。并发结束后状态必须符合串行语义。
	assert.False(t, s.activeHealthy, "activeHealthy must not flip under the alternating sequence")
	assert.Equal(t, 0, s.riseCount)
	assert.Equal(t, 1, s.fallCount)
	assert.NotNil(t, s.LastResult(), "LastResult must not be nil after the writer finishes")
}
