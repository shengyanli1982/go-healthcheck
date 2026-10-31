// Package state 维护每主机的健康状态机（内部实现包，消费者不可见）。
//
// 核心类型 HostState 以 per-host 互斥锁保护主动探测计数（rise/fall）与
// 被动镜像 bool，综合判定为确定性 AND；状态转换为纯函数：不读 wall-clock、
// 不读全局状态，相同输入事件序列必得相同最终状态。
//
// 状态归属约束（SF4/INV-6）：被动计数与窗口状态由 passive 包的
// PassiveDetector 实现独占，本包只保留镜像 bool，单向写入。
package state

import (
	"sync"

	"github.com/shengyanli1982/go-healthcheck/checker"
)

// HostState 维护单个主机的健康状态。
//
// 并发模型：per-host sync.Mutex 保护所有字段。
//   - 主动探测 worker 经 ApplyResult 原子更新 activeHealthy/riseCount/fallCount/lastResult
//   - 被动检测翻转时由根包经 SetPassiveHealthy 写 passiveHealthy（镜像 bool）
//   - 任何 goroutine 可自锁读 IsHealthy()/LastResult()，锁持有时间纳秒级
//
// SF4 修正：HostState 不存储被动计数/窗口状态，由 PassiveDetector 实现独占。
//
// 状态转换为纯函数：输出仅取决于当前内部状态 + 输入参数，无隐藏时序依赖、
// 无随机性。给定相同探测结果序列，必得相同最终状态。
//
// 导出面无持锁前置条件（G1）：所有导出方法自锁，跨包调用者无需（也无法）
// 持有非导出锁；包内无锁原语 recordSuccess/recordFailure/isHealthy 不导出。
type HostState struct {
	mu             sync.Mutex
	activeHealthy  bool                 // 初始 false（DOWN，对齐 nginx default_down 安全启动原则）
	passiveHealthy bool                 // 初始 true（无流量时不因被动而误判不健康）
	riseCount      int                  // 连续成功计数（达到 rise 阈值翻转 activeHealthy=true）
	fallCount      int                  // 连续失败计数（达到 fall 阈值翻转 activeHealthy=false）
	lastResult     *checker.CheckResult // INV-4：指针一旦赋值即不可变
}

// NewHostState 创建初始状态的 HostState：activeHealthy=false,
// passiveHealthy=true（docs/03 §1.2 冷启动语义：未经探测的主机不参与路由）。
func NewHostState() *HostState {
	return &HostState{passiveHealthy: true}
}

// ApplyResult 在一次 mu 临界区内原子完成：计数更新（按 result.Healthy 走
// recordSuccess/recordFailure）+ lastResult 写入（INV-4）+ 综合 healthy
// 快照读取（P0-3 锁内快照）。
//
// 返回：
//   - changed = ActiveHealthy 是否翻转（仅主动状态机翻转，不含被动镜像）
//   - healthy = 锁内综合快照（ActiveHealthy && PassiveHealthy）
//
// 注意：changed 与 SetPassiveHealthy 的 changed（综合翻转）语义不同。
func (s *HostState) ApplyResult(result *checker.CheckResult, rise, fall int) (changed bool, healthy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if result.Healthy {
		changed = s.recordSuccess(rise)
	} else {
		changed = s.recordFailure(fall)
	}
	s.lastResult = result
	return changed, s.isHealthy()
}

// SetPassiveHealthy 被动翻转路径：同一临界区写镜像 bool + 读综合快照。
//
// 返回：
//   - changed = 综合健康值（activeHealthy && passiveHealthy）是否因本次写入
//     发生翻转（docs/01 §4.2"检查综合状态是否翻转"）
//   - healthy = 锁内综合快照
//
// 注意：changed 与 ApplyResult 的 changed（ActiveHealthy 翻转）语义不同。
func (s *HostState) SetPassiveHealthy(v bool) (changed bool, healthy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	before := s.activeHealthy && s.passiveHealthy
	s.passiveHealthy = v
	healthy = s.activeHealthy && s.passiveHealthy
	return before != healthy, healthy
}

// IsHealthy 返回综合健康状态（确定性 AND：ActiveHealthy && PassiveHealthy）。
// 自锁版本，任意 goroutine 可直接调用。
func (s *HostState) IsHealthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isHealthy()
}

// LastResult 返回最近一次主动探测结果指针（不可变对象，R-d/INV-4），自锁读；
// 未探测过返回 nil。消费者可安全持有和读取返回指针。
func (s *HostState) LastResult() *checker.CheckResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastResult
}

// recordSuccess 记录一次主动探测成功（纯函数式转换，docs/03 §1.3）。
// 逻辑：riseCount++; fallCount=0（反向结果重置对侧计数器）;
// if !activeHealthy && riseCount>=rise -> 翻转 activeHealthy 为 true。
// 前置条件：调用者已持有 s.mu（包内无锁原语，G1：docs/02 原 RecordSuccess 降级）。
func (s *HostState) recordSuccess(rise int) (changed bool) {
	s.riseCount++
	s.fallCount = 0
	if !s.activeHealthy && s.riseCount >= rise {
		s.activeHealthy = true
		changed = true
	}
	return changed
}

// recordFailure 记录一次主动探测失败（纯函数式转换，docs/03 §1.3）。
// 逻辑：fallCount++; riseCount=0（反向结果重置对侧计数器）;
// if activeHealthy && fallCount>=fall -> 翻转 activeHealthy 为 false。
// 前置条件：调用者已持有 s.mu（包内无锁原语，G1：docs/02 原 RecordFailure 降级）。
func (s *HostState) recordFailure(fall int) (changed bool) {
	s.fallCount++
	s.riseCount = 0
	if s.activeHealthy && s.fallCount >= fall {
		s.activeHealthy = false
		changed = true
	}
	return changed
}

// isHealthy 综合健康判定（确定性 AND）。
// 前置条件：调用者已持有 s.mu（包内无锁原语）。
func (s *HostState) isHealthy() bool {
	return s.activeHealthy && s.passiveHealthy
}
