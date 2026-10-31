// Package passive 提供基于真实流量的被动健康检测（内部实现包 + 可替换接口）。
//
// 核心抽象为 PassiveDetector 接口（Strategy 模式）；内置唯一实现
// TimeWindowDetector 对齐 nginx max_fails/fail_timeout 语义。
// 被动计数与窗口状态由 PassiveDetector 实现独占（SF4），
// HealthManager 仅持有镜像 bool（HostState.PassiveHealthy）。
package passive

import "time"

// Clock 抽象时间来源（G2：消费者侧接口，结构化兼容根包 Clock——
// 根包以 Clock 接口值直接传参即满足本接口，避免 passive→根包循环 import）。
type Clock interface {
	Now() time.Time
}

// PassiveDetector 定义基于真实流量的被动健康检测行为。
// MVP 仅 TimeWindowDetector 一种实现；接口为 v1.1+ 演进预留。
// 设计模式：Strategy（被动检测算法可替换）。
//
// 状态归属约束（SF4）：
//
//	PassiveDetector 实现独占被动计数与窗口状态。
//	HostState 只保留镜像 bool PassiveHealthy。
//	禁止在 HostState 中存储 passiveCount/windowStart。
//
// 并发约束：
//
//	Record 可能被多个宿主 goroutine 并发调用。
//	实现必须自带内部同步。
//	禁止在持有 HostState.mu 时调用 PassiveDetector 的任何方法（避免嵌套锁）。
type PassiveDetector interface {
	// Record 记录一次请求结果（由宿主在真实流量路径中调用）。
	// 返回 passiveHealthy 是否发生翻转（true=翻转了）。
	// 实现内部使用注入的 Clock 获取时间戳。
	Record(host string, success bool) (flipped bool, newHealthy bool)

	// Reset 重置指定 host 的被动检测状态（host 下线/重新注册时调用）。
	Reset(host string)
}

// CurrentHealthyReader 是 PassiveDetector 的可选能力接口
// （r4 审计 P1-1 修复引入）：暴露 detector 对指定 host 的当前判定值，
// 供根包在被动翻转投递前复核——Record 判定与镜像写入/回调投递两阶段间，
// 并发 report 的翻转可能已先落地（detector 状态前移），本 goroutine 持有的
// Record 返回快照已 stale；直接投递会以旧值覆盖镜像，使镜像与 detector
// 永久分歧。实现本接口的 detector（如内置 TimeWindowDetector）由根包在
// cbMu 临界区内复核当前值替换 stale 快照，投递流静止后镜像终态收敛到
// detector 当前值。
//
// 实现约定：
//   - 返回值须与 Record 落地值同源同锁（同一内部锁保护），保证读取原子；
//   - 条目不存在时返回 (true, false)：true 为初值语义，ok=false 提示调用方
//     保持自身原值；
//   - 方法必须零分配、O(1)（处于被动翻转投递路径，虽低频但不引入分配）。
//
// 未实现本接口的自定义 detector 保持旧语义（report 直接投递 Record 快照），
// 慢回调场景可产生分歧窗口（见根包 StateChangeCallback godoc 的顺序契约）。
type CurrentHealthyReader interface {
	// CurrentHealthy 返回指定 host 的当前被动健康判定。
	// ok=false 表示条目不存在（无实测判定），调用方应保持自身原值。
	CurrentHealthy(host string) (healthy, ok bool)
}

// 编译期断言：内置 TimeWindowDetector 满足可选能力接口。
var _ CurrentHealthyReader = (*TimeWindowDetector)(nil)
