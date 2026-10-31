package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/shengyanli1982/workqueue/v2"
)

// timerQueueScheduler 是 Scheduler 的默认实现：workqueue/v2 幂等 TimerQueue
// + 集中式 Worker Pool（docs/04 §1）。
//
// 契约落点：
//   - SF1：队列必须以幂等模式构造（New 中幂等配置为副作用调用），
//     同一 host 在被 Get 取出后至 Done 之前不会被再次投递——单写者不重叠；
//     处理期间的重复 Put 仅留下挂起标记，Done 时重新入队。
//   - SF2：GetWithContext 不在 TimerQueue 接口内（BlockingGetQueue 可选接口），
//     构造期一次性类型断言获取（fail-fast，非运行时防御），失败返回 error；
//     关停经 TimerQueue 内嵌 Queue 提升的 Shutdown()，无需额外断言（R3）。
//   - SF3：Cancel 底层返回 false 是正常路径（已出堆搬入内层或未入堆），
//     交由根包墓碑检查兜底，不视为错误。
//
// 确定性承诺为事件序列级（SF5）：本实现不注入 Clock，调度时刻由
// TimerQueue 内部真实时间决定。
type timerQueueScheduler struct {
	tq workqueue.TimerQueue
	bq workqueue.BlockingGetQueue
	wg sync.WaitGroup // worker 生命周期
}

// 编译期断言：具体类型满足 Scheduler 接口。
var _ Scheduler = (*timerQueueScheduler)(nil)

// ErrNotBlockingQueue 由 New() 构造期类型断言失败时返回（SF2/G3：一次性
// fail-fast，非运行时防御）。消息自包含 "scheduler: " 前缀（docs/06 §5）。
//
// 触发条件：workqueue.TimerQueue 具体实现不满足 workqueue.BlockingGetQueue
// 可选接口——正常情况下断言必成功（timerQueueImpl 实现了 GetWithContext），
// 仅当依赖升级引入 bug（workqueue/v2 未来版本移除/改名该方法）时才会发生。
// 导出判据：消费者可能需要 errors.Is 判别以定位依赖升级问题（导出成本极低，
// 不因"内部性质"拒绝导出，docs/06 §5.1）；经根包 New() 以
// "healthcheck: scheduler init: %w" 包装传播（wrap 层次信息为上下文增量）。
var ErrNotBlockingQueue = errors.New("scheduler: timer queue does not implement BlockingGetQueue")

// New 构造幂等 TimerQueue 并完成可选接口断言（G3：构造期一次性，
// fail-fast，非运行时防御）。R3 修订：收敛为 BlockingGetQueue 单断言，关停改经
// TimerQueue 内嵌 Queue 提升的 Shutdown()（DrainableQueue 断言已删除，ADR-001 决策 #33）。
// error 由根包 New() 传播给消费者。
func New() (*timerQueueScheduler, error) {
	// —— P0 编译级契约三行（runbook §0.3，逐字）：
	// 幂等配置方法定义在 *QueueConfig（经内嵌提升），返回 *QueueConfig，
	// 为副作用调用，不可链式赋值；真实构造函数为 NewTimerQueue。
	cfg := workqueue.NewTimerQueueConfig()
	cfg.WithValueIdempotent()
	tq := workqueue.NewTimerQueue(cfg)

	bq, ok := tq.(workqueue.BlockingGetQueue)
	if !ok {
		return nil, ErrNotBlockingQueue // sentinel 直返（消息自包含前缀，docs/06 §5）
	}
	return &timerQueueScheduler{tq: tq, bq: bq}, nil
}

// Schedule 将 host 加入调度：PutAfter(host, offset)，首次调度锚点 = 调用时刻（R-a）。
// host 为调用方预装箱值（C2：动态类型契约见 Scheduler 接口 godoc），透传
// workqueue 不再装箱——消除原 string→any 每调用 16B 堆分配（pprof §4(4)）。
// ErrQueueIsClosed 在关停竞态下属正常路径，良性忽略。
// interval 参数不用于首次 PutAfter（后续重排经 Rearm），保留以符合
// docs/02 §5 接口签名并供根包 fakeScheduler 断言。
func (s *timerQueueScheduler) Schedule(host any, interval time.Duration, offset time.Duration) {
	_ = s.tq.PutAfter(host, offset)
}

// Cancel 从调度中移除 host（C2：预装箱值透传，同 Schedule）。底层 bool 返回值
// 丢弃：false 是正常路径（SF3：已出堆搬入内层或未入堆，交由墓碑检查兜底），
// 调用方不得视为错误。
func (s *timerQueueScheduler) Cancel(host any) {
	s.tq.Cancel(host)
}

// Run 启动 workers 个 goroutine 并发消费到期 host（非阻塞返回）。
// worker 循环按 docs/04 §4.1：GetWithContext 返回 error（ctx 取消或
// 队列关闭）即退出。probeFn 由根包提供，其收尾负责 Done + Rearm。
func (s *timerQueueScheduler) Run(ctx context.Context, workers int, probeFn func(host string)) {
	for i := 0; i < workers; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for {
				item, err := s.bq.GetWithContext(ctx)
				if err != nil {
					return // ctx 取消或队列关闭
				}
				// C2：到期项即调用方入队的预装箱值（动态类型恒 string，接口契约），
				// any→string 断言零分配；契约违反属编程错误，fail-loud panic。
				probeFn(item.(string))
			}
		}()
	}
}

// Shutdown 优雅关停：立即关闭队列停止调度（TimerQueue 提升的 Shutdown()，
// 阻塞中的 worker 经 GetWithContext 返回 ErrQueueIsClosed 退出），再在 ctx
// 约束下有界等待全部 worker 退出——即等待在途探测完成（成功路径保证
// M2-C8 无泄漏）。ctx 超时/取消返回 ctx.Err() 且不再等待（Stop 报错由
// 消费者处置，与原超时契约一致）。重复调用安全：底层关闭经 sync.Once
// 保护，已退出的 WaitGroup 等待即刻返回 nil。
//
// 裁定依据（docs/04 R3 / ADR-001 决策 #33）：旧机制 ShutdownWithDrain 在
// 根包 Stop 的 cancel-first 顺序下 drain 判定不可达——TimerQueue scheduler
// 协程 carry 到期项经内层 Queue.Put 直入、绕过 draining 拒绝（timer_queue.go
// 调度循环），而 cancel 后 worker 的 GetWithContext select 在 signal 与
// ctx.Done 同时就绪时随机选择、可能弃单退出，滞留项无 worker 消费，
// 必然卡满 ctx 超时后强制关闭并返回 spurious "context deadline exceeded"。
func (s *timerQueueScheduler) Shutdown(ctx context.Context) error {
	s.tq.Shutdown() // 立即关闭：停止调度，唤醒全部阻塞的 GetWithContext

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err() // 超时路径不再等待（与原契约一致）
	}
}

// Done 标记 host 探测完成，从 processing 集合移除（G3b：具体类型导出方法，
// 供根包 probeFn 收尾调用，INV-1b 先 Done 后 Rearm）。
// host 为调用方预装箱值（C2：动态类型契约见 Scheduler 接口 godoc），透传不装箱。
// 无 error 返回，不可能失败；重复调用或对非处理中值调用是安全 no-op。
func (s *timerQueueScheduler) Done(host any) {
	s.tq.Done(host)
}

// Rearm 在 interval 之后重新调度 host 的下一次探测（G3b：具体类型导出方法，
// 探测完成后调用——INV-1a 重排必须在探测之后，防探测风暴）。
// host 为调用方预装箱值（C2：透传不装箱，同 Done）。
// 关停/drain 期间 PutAfter 返回 ErrQueueIsClosed，良性忽略。
func (s *timerQueueScheduler) Rearm(host any, interval time.Duration) {
	_ = s.tq.PutAfter(host, interval)
}
