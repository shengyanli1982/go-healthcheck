package healthcheck

import "time"

// Clock 抽象时间来源，使所有时间相关行为可复现、可测试。
// 生产环境使用 realClock（time.Now）；测试注入 fakeClock 控制时间流速。
// 设计依据：硬约束 #10（确定性/可复现）。
// 覆盖范围（SF5 修正）：CheckResult.Timestamp、被动时间窗口起始/到期/恢复。
// 不覆盖：workqueue/v2 TimerQueue 的调度触发时刻（内部硬编码 time.Now）。
//
// 约束：
//   - Clock 实现必须是并发安全的（多 goroutine 调用 Now()）
//   - realClock 不导出，通过 New() 默认注入
//   - 消费者可通过 WithClock(c) 注入自定义实现
//
// 本接口与 passive.Clock 结构化兼容（duck-typing，G2）：
// 根包 Clock 实现可直接传入 passive.NewTimeWindowDetector，避免循环 import。
type Clock interface {
	Now() time.Time
}

// realClock 是 Clock 的默认实现（无状态，天然并发安全）。不导出；
// 由 newDefaultConfig 默认注入，消费者可经 WithClock 覆盖。
type realClock struct{}

// Now 返回当前时间（time.Now）。
func (realClock) Now() time.Time { return time.Now() }
