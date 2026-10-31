// Package scheduler 提供探测调度抽象与基于 workqueue/v2 幂等 TimerQueue 的默认实现。
//
// 本包是 go-healthcheck 的内部包，对消费者不可见：Scheduler 接口是内部
// 可测试性边界，不对外暴露 WithScheduler Option。
package scheduler

import (
	"context"
	"time"
)

// Scheduler 定义探测调度行为。
// 默认实现基于 workqueue/v2 TimerQueue + 确定性错峰。
// 注意：Scheduler 接口是内部可测试性边界，不对外暴露 WithScheduler Option。
//
// host 参数为预装箱值（C2 装箱消除）：动态类型必须是 string——worker 侧以
// item.(string) 断言取出（断言失败属编程错误，fail-loud panic）；workqueue
// matchTimerValue 的 string case 与幂等 Set 的 string 特化均为值比较，
// dedup/幂等语义与逐次装箱完全一致。调用方负责一次装箱后复用该 any 值
// （根包 managedHost.boxed），避免每次调用重复 string→any 堆分配（16B/次）。
type Scheduler interface {
	// Schedule 将 host 加入调度（动态类型契约见 Scheduler 接口 godoc）。
	// offset 为确定性错峰偏移量（由注册索引计算，非随机）。
	// 首次探测在 Schedule() 被调用的时刻 + offset 后触发（锚点 = 调用时刻，非 Start 时刻）。
	// 后续每 interval 触发一次（固定延迟语义：周期 = interval + 探测耗时）。
	Schedule(host any, interval time.Duration, offset time.Duration)

	// Cancel 从调度中移除 host（动态类型契约见 Scheduler 接口 godoc）。
	// 返回值说明（SF3）：底层 Cancel 返回 false 是正常路径（表示已出堆，交由墓碑检查兜底），
	// 不应视为错误。本接口不透出底层 bool 返回值。
	Cancel(host any)

	// Run 启动调度循环。
	// workers 个 goroutine 并发消费到期 host。
	// probeFn 由 HealthManager 提供，执行探测+状态更新。
	// ctx 取消时优雅退出。
	Run(ctx context.Context, workers int, probeFn func(host string))

	// Shutdown 优雅关停：立即关闭队列停止调度，在 ctx 约束下有界等待
	// 在途探测完成（worker 退出）；超时返回 error，重复调用安全（R3，
	// 见 ADR-001 决策 #33）。
	Shutdown(ctx context.Context) error
}
