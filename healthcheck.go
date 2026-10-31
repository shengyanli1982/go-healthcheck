// Package healthcheck 提供可嵌入的、协议无关的 upstream 健康检测纯库
// （主动探测 rise/fall 状态机 + 被动检测 nginx max_fails/fail_timeout 窗口）。
//
// 核心哲学：库输出事实，不输出决策——探测结果（CheckResult）与聚合健康信号
// （IsHealthy/GetHealthyHosts/StateChangeCallback）由本库产出；路由、负载均衡、
// 流量调度决策归宿主（docs/01 §1）。
//
// 基本用法（M1 四行接入）。WithProtocol 依赖协议子包 init() 注册，使用前必须
// blank import 对应子包（database/sql 驱动模式），否则 AddHost 返回
// protocol not registered 错误：
//
//	import (
//		"github.com/shengyanli1982/go-healthcheck/checker" // 协议名常量
//		_ "github.com/shengyanli1982/go-healthcheck/checker/http"
//	)
//
//	hc, _ := healthcheck.New()
//	_ = hc.AddHost("backend:8080", healthcheck.WithProtocol(checker.ProtocolHTTP))
//	_ = hc.Start(context.Background())
//	healthy := hc.GetHealthyHosts()
//
// 错误判别（docs/06 §5）：同步 API 返回的 error 均为导出 sentinel
// （ErrAlreadyStarted/ErrStopped/ErrHostExists 等）直返，或以 %w 附加
// host/protocol 上下文包装（统一后缀模板）；两种形态 errors.Is 均可稳定判别。
//
// 错误传播（docs/06）：库零日志，错误经四通道传播——同步 API error、
// CheckResult.Err、WithOnInternalError sink、StateChangeCallback。
// 全库恰好 2 处 recover，均在消费者代码边界（safeCheck 捕获 Checker panic、
// notifyLocked 捕获回调 panic）；库自身代码 fail-loud（决策 A），
// probeFn 的 defer 收尾保证 panic unwind 期间 Done+Rearm 仍执行。
package healthcheck

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/shengyanli1982/go-healthcheck/passive"
	"github.com/shengyanli1982/go-healthcheck/scheduler"
	"github.com/shengyanli1982/go-healthcheck/state"
)

// hostScheduler = docs/02 §5 Scheduler 接口 + G3b 收尾方法（Done/Rearm）。
// 具体类型 *scheduler.timerQueueScheduler 实现之（scheduler.New 返回）；
// 测试替身 fakeScheduler 实现之，用于不依赖真实 TimerQueue 断言调度时序（INV-1b 等）。
// 接口嵌入保证与 docs/02 §5 的 4 方法一致；Scheduler 是内部可测试性边界，
// 不对外暴露 WithScheduler Option。
// Done/Rearm 的 host 参数为预装箱值（C2：动态类型契约与装箱复用责任见
// scheduler.Scheduler 接口 godoc；根包侧复用 managedHost.boxed）。
type hostScheduler interface {
	scheduler.Scheduler

	// Done 标记 host 探测完成，从 processing 集合移除（INV-1b：先 Done 后 Rearm；
	// INV-2：probeFn 收尾无条件调用，不泄漏 processing）。无 error 返回，不可能失败。
	Done(host any)

	// Rearm 在 interval 之后重新调度 host 的下一次探测
	// （INV-1a：必须在探测完成之后调用——防探测风暴第一性要求）。
	Rearm(host any, interval time.Duration)
}

// managedHost 聚合单个 host 的全部运行时实体。
// cbMu 归属此处（G5，per-host，INV-7）：保证同一 host 的状态翻转事件串行投递；
// 在状态变更之前获取，使"翻转+快照+投递"成为同一临界区（C1 修复）。
// 全局锁序（docs/03 §3.2）：cbMu → manager.mu → HostState.mu，单向无环；
// PassiveDetector 内部锁为 leaf lock，不参与锁链（唯一登记例外：report 的
// identity 失配路径持 cbMu 时 Reset 孤儿条目——detector 锁 leaf 性质保证
// cbMu → detectorLock 单向无环，见 docs/03 §3.2）。
// cfg/boxed 在 AddHost 写入后不可变（无 setter），故 RUnlock 后读这些字段无 race。
type managedHost struct {
	cbMu sync.Mutex // 同 host 回调串行（INV-7）；非重入
	cfg  hostConfig // 已解析：checker 非 nil、interval/timeout 为有效值（host 覆盖已应用）
	hs   *state.HostState
	// boxed 是 host 字符串的一次性装箱值（C2：AddHost 期写入，动态类型恒 string）。
	// Schedule/Done/Rearm/Cancel 全路径复用本值透传 workqueue（any 形参），消除
	// 热路径每探测 2 次 string→any 重复装箱（各 16B）；workqueue 值比较语义下
	// 与逐次装箱完全等价（matchTimerValue string case / 幂等 Set string 特化）。
	boxed any
}

// probeParams 是 probeFn 步骤 1 单次查找的返回快照（docs/04 §4.2 N4）：
// 墓碑检查与探测参数解析合并为一次 manager.mu.RLock 查找，返回 (cfg, ok)，
// ok=false 即墓碑（INV-3），杜绝 host 已移除时返回值未定义。
type probeParams struct {
	checker  checker.Checker
	timeout  time.Duration
	interval time.Duration
	rise     int
	fall     int
}

// HealthManager 是 go-healthcheck 的核心入口（Facade 模式）。
// 对外提供统一 API，内部组装 Checker + State + PassiveDetector + Scheduler + Clock。
//
// 并发安全：所有公开方法均可从任意 goroutine 调用。
type HealthManager struct {
	hosts           map[string]*managedHost
	nextIndex       int // 下一个注册索引（单调递增，不回收；确定性错峰 slot = index % spreadSlots 的输入）
	sched           hostScheduler
	passive         passive.PassiveDetector
	clock           Clock
	config          managerConfig
	onChange        StateChangeCallback
	onInternalError InternalErrorHandler // 可选，默认 nil（回调 panic 被隔离并丢弃）
	mu              sync.RWMutex         // 保护 hosts/nextIndex/started/stopped/cancel
	cancel          context.CancelFunc   // 内部 context 取消（Start 时创建）
	started         bool
	stopped         bool // Stop 置位（无论是否 Start 过，ERC P2-2）；置位后 Start/AddHost fail-fast（生命周期一次性）
}

// New 创建 HealthManager 实例。零配置即可生产使用（所有参数有合理默认值）。
// 配置非法时直接返回 error（fail-fast，不静默回退默认值）。
//
// G3：workqueue 可选接口类型断言在 scheduler.New() 构造期一次性完成，
// 断言失败的 error 经本函数传播给消费者（初始化边界校验，非运行时防御）。
// G2：未注入 WithPassiveDetector 时，以 passiveMaxFails/passiveFailTimeout
// 与注入 Clock 构造默认 *passive.TimeWindowDetector（根包 Clock 接口值
// 结构化满足 passive.Clock，duck-typing）。
func New(opts ...Option) (*HealthManager, error) {
	cfg := newDefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err // fail-fast，不静默回退
	}
	sched, err := scheduler.New()
	if err != nil {
		return nil, fmt.Errorf("healthcheck: scheduler init: %w", err) // G3：构造期断言 error 在此传播
	}
	det := cfg.passiveDetector
	if det == nil {
		// G2：根包 Clock 接口值直接满足 passive.Clock（结构化 duck-typing）
		det = passive.NewTimeWindowDetector(cfg.passiveMaxFails, cfg.passiveFailTimeout, cfg.clock)
	}
	return &HealthManager{
		hosts:           map[string]*managedHost{},
		sched:           sched,
		passive:         det,
		clock:           cfg.clock,
		config:          *cfg,
		onChange:        cfg.onChange,
		onInternalError: cfg.onInternalError,
	}, nil
}

// Start 启动健康检测调度（非阻塞）。
// 同步校验 fail-fast：重复启动返回 error（含 already started）；Stop 之后启动
// 返回 error（manager stopped——生命周期一次性，Stop 后不可重新 Start，
// 与 AddHost after Stop 对齐，ERC P2-2）；ctx 已取消返回 error（P3-3 边界校验，
// 防静默产生永久失能的 manager；wrap 保留 errors.Is(err, context.Canceled) 判别，
// 校验失败不消耗一次性生命周期，可用合法 ctx 重试）。
// workqueue 可选接口类型断言已在 New() 构造期一次性完成（G3），Start 只管
// 生命周期校验 + worker 启动；worker 启动失败语义由 Scheduler.Run 契约保证
// （非阻塞启动 workers 个 goroutine，ctx 取消时优雅退出）。
// 父 ctx 取消仅使 workers 退出，不回收 New() 构造期启动的 TimerQueue 搬运协程
// 与堆内存——资源回收必须调用 Stop，取消不替代 Stop（2026-10-30 审计 P3-7）。
// 父 ctx 在运行中取消（非经 Stop）→ 主动探测零信号停摆（workers 退出，探测
// 不再发生，无错误、无日志）、IsHealthy 冻结在最后一次探测值，而被动路径
// 仍然存活（ReportSuccess/ReportFailure 照常驱动镜像与翻转回调投递）；
// 请传入与 manager 生命周期一致的 ctx，取消后调用 Stop 收尾（P3-2）。
//
// probeFn 经闭包适配 Scheduler.Run 的 func(host string) 参数（M-3）：
// 内部 ctx 显式传参，消除隐藏字段的测试 nil 陷阱。
func (m *HealthManager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return ErrStopped // 生命周期一次性：Stop 后拒绝 Start（ERC P2-2）；sentinel 直返
	}
	if m.started {
		return ErrAlreadyStarted // sentinel 消息自包含前缀，直返（docs/06 §5）
	}
	// 边界校验（P3-3）：已取消的 ctx 会静默产生永久失能的 manager——workers 经
	// GetWithContext 立即全部退出，hosts 永不被探测，且 started=true 使重复 Start
	// 只报 ErrAlreadyStarted，四错误通道全部静默（零日志放大）。fail-fast 拒绝；
	// 校验失败不消耗一次性生命周期（started 未置位，可用合法 ctx 重试）。
	// wrap 保留 errors.Is(err, context.Canceled) 判别（stdlib 惯例）；
	// 前缀+操作语境是真实上下文增量（docs/06 §5.1 规则 3），不新增 sentinel。
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("healthcheck: start with canceled context: %w", err)
	}
	internalCtx, cancel := context.WithCancel(ctx)
	m.cancel, m.started = cancel, true
	m.sched.Run(internalCtx, m.config.workers, func(host string) { m.probeFn(internalCtx, host) }) // M-3 闭包适配
	return nil
}

// Stop 优雅关停：取消调度 -> 等待在途探测完成 -> 释放资源。
// 关停后 IsHealthy/GetHealthyHosts/GetLastResult 仍可查询（优雅降级，hosts 状态不清空）。
// 首次 Stop 成功返回（nil）后无 goroutine 泄漏（Scheduler.Shutdown 成功路径等待
// 全部 worker 退出）；并发/重复调用者经 stopped 幂等短路立即返回，其返回时刻
// 不承诺关停已完成（2026-10-30 审计 P3-1/5b godoc 弱化路线）。
//
// 生命周期一次性（ERC P2-2）：Stop 无论是否 Start 过均置 stopped 并执行
// Scheduler.Shutdown；此后 Start/AddHost 均返回 manager stopped。
// 未 Start 直接 Stop 不是 no-op——New() 构造期即启动 workqueue TimerQueue 的
// 内部 scheduler 搬运 goroutine，Shutdown 是唯一回收点，跳过则永久泄漏该 goroutine；
// 未 Run 时 workers WaitGroup 为空，Shutdown 的有界等待即刻返回。
// 重复 Stop 幂等返回 nil（已 stopped 直接返回，不再 cancel/Shutdown，
// 亦不再传播首次调用的 error）。
//
// 内部流程（docs/04 §6.1）：
// 已 Start 时先取消内部 context（workers 经 GetWithContext 退出；在途探测的 probeCtx 同被取消），
// 再经 Scheduler.Shutdown 立即关闭队列停止调度、有界等待在途探测完成（worker 退出）；超时返回 error。
// 回调禁区内调用 Stop 会自死锁（Shutdown 等待 worker 退出而 worker 正被回调占用，见 StateChangeCallback godoc）。
func (m *HealthManager) Stop(ctx context.Context) error {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return nil // 重复 Stop 幂等（ERC P2-2）：已关停，直接返回 nil
	}
	m.stopped = true // 生命周期一次性：无论是否 Start 过，此后 Start/AddHost fail-fast
	started, cancel := m.started, m.cancel
	m.mu.Unlock()
	if started {
		cancel() // 1. 取消内部 ctx（workers 经 GetWithContext 退出；在途探测的 probeCtx 同被取消）
	}
	// 2. Shutdown 立即关闭队列 + 回收 New() 构造期启动的 scheduler 搬运 goroutine
	//    （ERC P2-2：未 Start 路径同样必须执行；未 Run 时 wg 空、有界等待即刻返回）；
	//    在途探测经有界等待完成（worker 退出）；超时 → error
	return m.sched.Shutdown(ctx)
}

// AddHost 注册一个待检测主机。
// 内部分配注册索引，计算确定性错峰 offset，加入调度。
// host 为空字符串时返回 error（唯一的参数校验点）。
// host 已存在时返回 error（重复注册保护）。
// Stop 后调用返回 error（manager stopped，生命周期一次性 fail-fast，L5 P2-2a；
// AddHost×Stop 并发竞态窗口仍由 Schedule 的 ErrQueueIsClosed 良性忽略兜底，docs/04 SF3）。
//
// Checker 解析：WithChecker > WithProtocol（互斥优先级）；二者皆无返回 error；
// 协议未注册时返回 error（Registry 语义，不 panic）。host 级 interval/timeout
// 显式覆盖经 hcfg.validate(host) 范围校验（Want-Shift R2，ST-06 契约；
// 零值 = 未覆盖哨兵，解析为全局值）。
//
// 确定性错峰（docs/04 §3.1，禁用随机数、非随机确定性）：slot = index % spreadSlots；
// offset = interval * slot / spreadSlots；锚点 = Schedule 调用时刻（R-a）。
// 注册索引单调递增不回收（RemoveHost 后再注册索引继续前进）。
// AddHost 后立即可查：冷启动 IsHealthy==false（M2-C2，default_down 安全启动）。
func (m *HealthManager) AddHost(host string, opts ...HostOption) error {
	if host == "" {
		return ErrEmptyHost // 唯一参数校验点；sentinel 直返（消息自包含前缀，docs/06 §5）
	}
	var hcfg hostConfig
	for _, opt := range opts {
		opt(&hcfg)
	}
	if err := hcfg.validate(host); err != nil {
		return err // Want-Shift R2：host 级 interval/timeout 范围（零值=未覆盖哨兵合法）
	}
	interval := m.config.interval
	if hcfg.interval > 0 {
		interval = hcfg.interval
	}
	timeout := m.config.timeout
	if hcfg.timeout > 0 {
		timeout = hcfg.timeout
	}
	c := hcfg.checker // WithChecker > WithProtocol（互斥优先级）
	if c == nil {
		if hcfg.protocol == "" {
			return fmt.Errorf("%w (host %q)", ErrNoChecker, host) // host 名为上下文增量（统一后缀模板，docs/06 §5）
		}
		var err error
		c, err = checker.NewChecker(hcfg.protocol, checker.TargetConfig{Addr: host, Timeout: timeout, Extra: nil})
		if err != nil {
			return fmt.Errorf("%w (host %q)", err, host) // 传播 registry/factory 错误，附加 host 上下文
		}
	}
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return ErrStopped // 生命周期一次性 fail-fast（L5 P2-2a）；sentinel 直返
	}
	if _, exists := m.hosts[host]; exists {
		m.mu.Unlock()
		return fmt.Errorf("%w (host %q)", ErrHostExists, host) // host 名为上下文增量
	}
	index := m.nextIndex
	m.nextIndex++ // 单调递增，不回收
	mh := &managedHost{
		cfg:   hostConfig{checker: c, interval: interval, timeout: timeout}, // 已解析快照，写入后不可变
		hs:    state.NewHostState(),
		boxed: host, // C2：注册期一次装箱，此后调度全路径复用（热路径零装箱）
	}
	m.hosts[host] = mh
	m.mu.Unlock()
	// 确定性错峰（docs/04 §3.1；禁用随机数）：slot = index % spreadSlots; offset = interval * slot / spreadSlots
	offset := interval * time.Duration(index%m.config.spreadSlots) / time.Duration(m.config.spreadSlots)
	m.sched.Schedule(mh.boxed, interval, offset) // 锚点=调用时刻（R-a）；锁外调用（不持 manager.mu）
	return nil
}

// RemoveHost 移除一个主机（停止探测 + 清理状态 + 重置被动检测）。
// host 不存在时返回 error。
// 内部调用 Scheduler.Cancel + PassiveDetector.Reset。
// SF3 约束：Cancel 返回 false 是正常路径（已出堆搬入内层或未入堆），
// 在途探测由 probeFn 墓碑检查兜底（INV-3）。
// Reset 防被动状态泄漏（docs/07 §3）；detector 内部锁为 leaf lock，
// 调用时不持任何其他锁。
func (m *HealthManager) RemoveHost(host string) error {
	m.mu.Lock()
	mh, ok := m.hosts[host] // 既有存在性查找顺带捕获句柄（C2：复用 mh.boxed，不新增状态）
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("%w (host %q)", ErrHostNotFound, host) // host 名为上下文增量
	}
	delete(m.hosts, host) // 墓碑即刻生效
	m.mu.Unlock()
	m.sched.Cancel(mh.boxed) // C2：复用注册期装箱值（写入后不可变，经 m.mu 建立 happens-before）；底层 false 是正常路径（SF3），接口无返回值
	m.passive.Reset(host)    // 防被动状态泄漏；leaf lock，调用时不持任何其他锁
	return nil               // 在途探测由 probeFn 墓碑检查兜底
}

// IsHealthy 返回指定主机的综合健康状态（ActiveHealthy && PassiveHealthy）。
// host 不存在时返回 false。
// 锁路径（docs/01 §4.3）：manager.mu.RLock 查找 → RUnlock → HostState 自锁读
// 两个 bool；不触碰 PassiveDetector（SF4：查询路径 O(1) 且无跨模块锁）。
// 回调白名单内（可在 StateChangeCallback 中调用）。
func (m *HealthManager) IsHealthy(host string) bool {
	mh, ok := m.getManagedHost(host)
	if !ok {
		return false
	}
	return mh.hs.IsHealthy() // G1 自锁读
}

// GetHealthyHosts 返回所有健康主机的地址列表。
// 确定性保证：按 addr 字典序排序（sort.Strings），禁止依赖 map 迭代随机序（C4）。
// 定位为快照/控制面 API，不建议在每请求热路径中调用；
// 热路径应经 WithOnStateChange 回调维护消费者自己的健康列表缓存。
// 锁路径：manager.mu.RLock 遍历 → 逐个 HostState 自锁读（锁序 manager.mu →
// HostState.mu 合法方向）→ RUnlock → 锁外排序。
// 回调白名单内（可在 StateChangeCallback 中调用）。
func (m *HealthManager) GetHealthyHosts() []string {
	m.mu.RLock()
	healthy := make([]string, 0, len(m.hosts))
	for host, mh := range m.hosts {
		if mh.hs.IsHealthy() {
			healthy = append(healthy, host)
		}
	}
	m.mu.RUnlock()
	sort.Strings(healthy) // C4 字典序，禁 map 迭代序
	return healthy
}

// GetLastResult 返回指定主机最近一次主动探测的 CheckResult。
// host 不存在或尚未探测过时返回 nil。
// 返回的指针指向不可变对象（R-d/INV-4：每轮探测新建、发布后不可变），
// 消费者可安全持有和读取。
// 回调白名单内（可在 StateChangeCallback 中调用）。
func (m *HealthManager) GetLastResult(host string) *CheckResult {
	mh, ok := m.getManagedHost(host)
	if !ok {
		return nil
	}
	return mh.hs.LastResult() // G1 自锁读
}

// Hosts 返回已注册主机数量。
// 回调白名单内（可在 StateChangeCallback 中调用）。
func (m *HealthManager) Hosts() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.hosts)
}

// ReportSuccess 宿主报告一次真实流量请求成功（驱动被动检测）。
// O(1)、零分配，处于宿主请求热路径。
// 未知 host 静默返回（热路径不校验，无 error 返回值；先查 map 后 Record）。
// 与 RemoveHost 并发竞态窗口内的上报经 cbMu 取得后的指针同一性复核丢弃
// （幽灵回调防护，ERC P2-A，docs/03 INV-3）。
// Stop 后被动上报仍有效（镜像更新与翻转回调投递继续，report 不检查 stopped）——
// 优雅 drain 期宿主可能仍在服务流量（2026-10-30 审计 P3-5，docs/06 §1.1）。
// 回调禁区内调用会因非重入 cbMu 自死锁（见 StateChangeCallback godoc）。
func (m *HealthManager) ReportSuccess(host string) { m.report(host, true) }

// ReportFailure 宿主报告一次真实流量请求失败（驱动被动检测）。
// O(1)、零分配，处于宿主请求热路径。
// 未知 host 静默返回（热路径不校验，无 error 返回值；先查 map 后 Record）。
// 与 RemoveHost 并发竞态窗口内的上报经 cbMu 取得后的指针同一性复核丢弃
// （幽灵回调防护，ERC P2-A，docs/03 INV-3）。
// Stop 后被动上报仍有效（镜像更新与翻转回调投递继续，report 不检查 stopped）——
// 优雅 drain 期宿主可能仍在服务流量（2026-10-30 审计 P3-5，docs/06 §1.1）。
// 回调禁区内调用会因非重入 cbMu 自死锁（见 StateChangeCallback godoc）。
func (m *HealthManager) ReportFailure(host string) { m.report(host, false) }

// report 是被动上报公共路径（热路径 O(1) 零分配；锁序 W3 逐字落实，docs/03 §3.2 条 3/7）：
// manager.mu.RLock 查找立即 RUnlock（只带指针）→ detector.Record（leaf lock，
// 调用时不持任何锁）→ 翻转后才取 cbMu（cbMu 在状态变更之前获取，翻转+快照+
// 投递同一临界区）→ 指针同一性复核（被动路径对称守卫，ERC P2-A）→
// 投递前复核 detector 当前值（可选接口 CurrentHealthyReader，r4 P1-1——
// stale 快照覆盖治本，见下方复核注释）→ SetPassiveHealthy（HostState.mu）→
// notifyLocked（已持 cbMu）。
// 复核失配（host 已删除或同名重注册，不区分形态）→ 无条件 Reset(host) 回收
// 本次 Record 惰性复活/翻转污染的 detector 条目并丢弃投递（P1-3 + ERC Phase4 R1：
// stale Record 先于 identity 复核执行，重注册豁免 Reset 会使翻转残余污染新世代
// 条目——镜像(true)/detector(false) 分歧在持续失败流量下无界存续；无条件 Reset
// 的误清成本为有界瞬态（新世代计数从 0 重建，至多延迟 maxFails-1 次上报），
// 方向安全。持 cbMu 取 detector leaf 锁为 docs/03 §3.2 登记的唯一锁序例外）。
// 禁止持 manager.mu 取 cbMu；正常路径无 fmt/切片分配（失配 Reset 为低频清理路径）。
func (m *HealthManager) report(host string, success bool) {
	m.mu.RLock()
	mh, ok := m.hosts[host]
	m.mu.RUnlock() // W3：查找后立即释放（只带指针）；禁止持 manager.mu 取 cbMu
	if !ok {
		return // 不存在静默返回（docs/05 §5 热路径不校验；无 error 返回值）
	}
	flipped, newHealthy := m.passive.Record(host, success) // leaf lock：调用时不持任何锁；detector 内无回调
	if !flipped {
		return
	}
	mh.cbMu.Lock()
	defer mh.cbMu.Unlock() // cbMu 在状态变更之前获取（翻转+快照+投递同一临界区）
	// 被动路径对称守卫（ERC P2-A，与主动路径 R4 指针同一性守卫同型，docs/03 INV-3）：
	// 查找→cbMu 获取之间的窗口可无界（cbMu 等待可被慢回调拉长），期间 host 可能被
	// RemoveHost（detector 已 Reset）或 RemoveHost→AddHost 同名重注册（新 *managedHost）。
	// 取得 cbMu 后复核 hosts[host] 仍指向 mh；失配（任一形态）→ 无条件 Reset + 丢弃
	// 投递（P1-3 修复；ERC Phase4 R1 裁定撤销"重注册豁免 Reset"两形态分支）：
	// 本路径的 Record 先于 identity 复核执行——慢回调拉长 cbMu 等待、窗口内
	// RemoveHost→AddHost 交错时，stale 报告的翻转残余已污染新世代同名 detector 条目
	// （一旦 passiveHealthy=false，失败路径永不再翻转、镜像(true)/detector(false)
	// 分歧在持续失败流量下无界存续 = P1-3 危害链；已删除形态下 map 条目永不回收）。
	// 危险不对称裁定：无条件 Reset 的误清成本 = 新注册在途计数被瞬时清零、惰性重建
	// （有界瞬态，至多延迟 maxFails-1 次真实失败上报，偏差方向保守 false-down）；
	// 豁免 Reset 的成本 = false-up 分歧无限期存续（被动快速驱逐静默失效）。
	// 失配路径不写镜像 bool、不投递幽灵回调，杜绝已移除/已重注册 host 的状态残留
	// 投递与 IsHealthy 查询自相矛盾。
	// 锁序合法：cbMu → manager.mu（既定单向锁序，hostIdentity 仅 RLock 比对指针）；
	// 失配 Reset 在持 cbMu 时取 detector leaf 锁——docs/03 §3.2 登记的唯一例外：
	// detector 锁内部从不获取任何其他锁（leaf 性质），且全库无"取 detector 锁后再取
	// cbMu/manager.mu/HostState.mu"的反向路径，cbMu → detectorLock 单向无环、无死锁。
	// 残余边界（docs/03 INV-3 登记）：复核→Reset 之间并发 AddHost+新注册 Record 的
	// TOCTOU 微窗口属瞬态自愈（误清的新条目由下次 Record 惰性重建，镜像/detector
	// 均 fresh 一致）；Record 翻转→Reset 落盘之间条目短暂处于翻转态，Reset 即回收。
	if !m.hostIdentity(host, mh) {
		m.passive.Reset(host) // P1-3/ERC-R1：失配无条件回收孤儿/stale 翻转条目（docs/03 §3.2 例外）
		return
	}
	// 投递前复核（r4 审计 P1-1「stale 快照覆盖」治本）：Record 判定与 cbMu 获取
	// 两阶段间，并发 report 的翻转可能已先落地投递（慢回调把 G1 的 cbMu 等待
	// 无界拉长、G2 插队完成投递，detector 状态前移），本 goroutine 的 newHealthy
	// 快照已 stale——直接投递会以旧值覆盖镜像，使 detector(true)/镜像(false)
	// 永久分歧（持续成功流量下 Record 恒 !flipped 早退，全库再无写镜像路径，
	// 独立复现实测 2000 迭代 6.4% 分歧率）。detector 实现可选接口
	// CurrentHealthyReader 时，在持 cbMu 的此刻复核 detector 当前值替换
	// stale 快照——消灭「晚判定早投递」的覆盖，投递流静止后镜像终态收敛到
	// detector 当前值；未实现接口或条目不存在（ok=false）时保持 Record 快照
	// （自定义 detector 退化旧语义，分歧窗口由 godoc 承诺如实描述）。
	// 断言仅在翻转路径执行（低频；非翻转早退在前），接口到接口断言经全局
	// itab 缓存零分配；锁序与失配 Reset 同型：持 cbMu 取 detector leaf lock，
	// docs/03 §3.2 登记的唯一锁序例外（detector 锁内部从不获取任何其他锁，
	// 单向无环、无死锁）。
	if r, implements := m.passive.(passive.CurrentHealthyReader); implements {
		if cur, exists := r.CurrentHealthy(host); exists {
			newHealthy = cur
		}
	}
	changed, healthy := mh.hs.SetPassiveHealthy(newHealthy) // G1：changed=综合翻转（docs/01 §4.2）
	if changed {
		m.notifyLocked(host, healthy, nil) // 被动翻转 result=nil（R-d）；notifyLocked 假定已持 cbMu
	}
}

// probeFn 执行一轮主动探测（docs/04 §4.2 伪代码逐行落实；M-3 显式 ctx 传参；
// 全库正确性核心）。由 Scheduler.Run 的 worker 经 Start 内闭包调用；
// 同包测试可直调（小写方法 seam，ST-08 oracle/确定性测试依赖，必须保留）。
//
// 不变式锚点：
//   - INV-1a：Rearm（PutAfter）必须在探测完成之后发出（防探测风暴第一性要求）
//   - INV-1b：收尾先 Done 后 Rearm（robustness，顺序不可颠倒）
//   - INV-2：Done 由外层 defer 收尾块无条件兜底，不泄漏 processing
//   - INV-3：步骤 1 墓碑检查（hostParams 单次查找 ok=false 即墓碑）+ 步骤 3/5 写结果/收尾前指针同一性判活
//     （步骤 3 的复核位于 cbMu 临界区内——P1-1 修复，与 report 被动路径守卫对称，
//     消除"锁外检查通过→cbMu.Lock 被慢回调无界拉长→窗口内 RemoveHost"的幽灵投递窗口）
//   - 取消伪影丢弃（P1-2）：步骤 2 返回后父 ctx 已取消（Stop 的 cancel 连带取消在途
//     probeCtx）→ 结果丢弃不计入状态机、不投递回调；defer 收尾照常执行
//   - R4（L5 P2-2）：指针同一性即注册代语义——步骤 3/5/收尾校验 hosts[host] 仍指向
//     步骤 1 捕获的 *managedHost；在途探测期间 RemoveHost→AddHost 同名重注册产生新指针，
//     旧注册的探测结果被丢弃（视同墓碑）且不 Rearm（新注册自有 Schedule 链）
//   - INV-4：ApplyResult 一次临界区完成计数更新+LastResult 写入（result 新建且不可变）
//   - INV-5：回调不在 HostState.mu 内（ApplyResult 返回即已释放）
//   - C1：cbMu 在状态变更之前获取，翻转+快照+投递同一临界区（G5：cbMu 归 managedHost）
//   - C3：cbMu 临界区收敛为步骤 3+4（立即调用闭包 + 闭包内 defer unlock，任何出口
//     恰好释放一次）；步骤 5 Done/Rearm 在 cbMu 之外执行——消除「cbMu → workqueue
//     全局锁」双重排队，正常路径与 panic 路径的收尾形态一致
//   - N4：hostParams 合并墓碑检查与参数解析为单次查找，返回 (mh, cfg, ok)
//   - S2：finished 语义 =「Done 与 Rearm 均完成」，两段式标志防 defer 重复收尾
//   - P0-3：healthy 为锁内快照（ApplyResult 返回值），非锁外重读
//
// panic 政策（决策 A fail-loud）：外层 defer 收尾块内没有 recover——库自身
// panic 向上传播崩溃宿主进程，但 defer 收尾在 unwind 期间仍执行 Done+Rearm
// （Go 语义保证），不泄漏 processing、不停探、不阻塞 Stop 的有界等待（R3）。
// 全库 recover 仅 2 处：safeCheck（消费者 Checker 边界）与 notifyLocked（消费者回调边界）。
func (m *HealthManager) probeFn(ctx context.Context, host string) {
	// —— 外层 defer 收尾块：覆盖 Done+Rearm 整个收尾；panic unwind 期间仍执行（Go 语义）。
	//    块内没有 recover——库自身 fail-loud（铁律 3）。finished 语义="Done 与 Rearm 均完成"（S2）。
	var pp probeParams
	var mh *managedHost // 步骤 1 捕获的注册代锚点（R4：指针同一性判定；墓碑路径恒 nil）
	finished := false
	defer func() {
		if !finished {
			// C2：优先复用注册期装箱值 mh.boxed（零分配）；墓碑路径 mh==nil 无句柄
			// 可复用，现场装箱 host（低频路径，裁定接受——不为此增加状态字段）。
			if mh == nil {
				m.sched.Done(host) // INV-2：无条件，不泄漏 processing
			} else {
				m.sched.Done(mh.boxed)
			}
			if m.hostIdentity(host, mh) { // R4：判活+同注册判定后才读 pp.interval（N4：pp/mh 经闭包引用）
				m.sched.Rearm(mh.boxed, pp.interval) // 不静默停探（hostIdentity true 蕴含 mh 非 nil）
			}
		}
	}()

	// 步骤 1：墓碑检查与 hostParams 合并为单次查找返回 (mh, cfg, ok)（P0，N4；ok=false 即墓碑 INV-3）；
	// mh 即本次注册的指针同一性锚点（R4）
	var ok bool
	mh, pp, ok = m.hostParams(host)
	if !ok {
		return // defer 收尾仍执行 Done（hostIdentity 对 nil 锚点恒 false → 不 Rearm）
	}

	// 步骤 2：执行探测（safeCheck = 全库 recover 点 (1)）
	probeCtx, cancel := context.WithTimeout(ctx, pp.timeout)
	defer cancel()
	result := m.safeCheck(pp.checker, probeCtx)

	// 取消伪影丢弃（P1-2，docs/04 §4.2/§6.1）：Stop 第一步的 cancel 会连带取消在途
	// probeCtx，Checker 返回事实化失败（Healthy=false + Err=context.Canceled）——这是
	// 关停伪影而非真实探测失败。检查父 ctx（internalCtx）精确区分两种失败形态：
	// 父已取消 = 关停伪影 → 丢弃结果（不计入 rise/fall、不投递关停期幽灵回调、
	// 不污染"Stop 后仍可查询"状态）；仅 probeCtx 超时（父未取消）才是真实失败。
	// defer 收尾（Done/Rearm）照常执行——真实 Stop 场景队列已 Shutdown，
	// Rearm 的 PutAfter 返回 ErrQueueIsClosed 良性忽略（docs/04 §2.3）。
	if ctx.Err() != nil {
		return
	}

	// 步骤 3+4（C1 修复：翻转+快照+投递在同一 cbMu 临界区；G5：cbMu 归 managedHost）
	// M-4 判活守卫经 R4 升级为指针同一性判定，并经 P1-1 修复移入 cbMu 临界区内复核
	// （与 report 路径的被动对称守卫 ERC P2-A 同型）：cbMu 之外检查通过后，cbMu.Lock
	// 可被慢回调无界拉长（上界=消费者回调时长），期间 RemoveHost（不需要 cbMu）或
	// RemoveHost→AddHost 同名重注册落地，照常写入+投递即产生孤儿状态写入与幽灵翻转。
	// 临界区内复核把窗口收敛回"复核→写入"纳秒级；锁外不保留重复检查（每探测省一次
	// manager.mu RLock，热路径友好）。hosts[host] != mh 即本次探测属旧注册或已删除，
	// 结果丢弃（视同墓碑），杜绝旧结果写入新注册/孤儿 HostState 与幽灵回调投递。
	//
	// C3（临界区收敛）：步骤 3+4 封装于立即调用闭包，cbMu 生命周期 = 闭包体；
	// 闭包内 defer unlock 保证 identity 失配提前 return 与 panic unwind 均恰好释放
	// 一次（唯一 unlock 注册点，无双重释放可能）。步骤 5 的 Done/Rearm 移至闭包外
	// （cbMu 之外）执行，消除「cbMu → workqueue 全局锁」双重排队（r5 §2.1：cbMu
	// 锁延迟占 ParallelHosts 31.3%）；正常路径收尾自此与 panic 路径形态一致
	// （外层 defer 收尾本就在 cbMu 释放后运行）。返回值 = 临界区内 identity 复核结果。
	proceed := func() bool {
		mh.cbMu.Lock()         // cbMu 在状态变更之前获取（锁序 cbMu → manager.mu → HostState.mu）
		defer mh.cbMu.Unlock() // 任何出口（含 panic unwind）恰好释放一次
		if !m.hostIdentity(host, mh) {
			return false // R4 临界区内复核失配：丢弃结果；外层 defer 收尾仍执行 Done（identity false → 不 Rearm）
		}
		// G1 组合方法：一次 HostState.mu 临界区完成 计数更新+LastResult 写入（INV-4）+锁内快照（P0-3）
		changed, healthy := mh.hs.ApplyResult(result, pp.rise, pp.fall)
		if changed {
			m.notifyLocked(host, healthy, result) // recover 点 (2)；INV-5：回调不在 HostState.mu 内（ApplyResult 已释放）；调用者已持 cbMu
		}
		return true
	}()
	if !proceed {
		return
	}

	// 步骤 5：收尾（INV-1a 重排在探测完成之后；INV-1b 先 Done 后 Rearm/PutAfter，顺序不可颠倒）
	// C3：cbMu 已随步骤 3+4 闭包返回释放——Done/Rearm 不需要 cbMu，正常路径与
	// panic 路径的收尾均在锁外；INV-1a/1b 由本 goroutine 程序序保证，不依赖 cbMu。
	// C2：mh.boxed 为注册期一次装箱值（步骤 1 已保证 mh 非 nil），热路径零装箱；
	// 同名重注册场景下新旧 boxed 值比较相等，workqueue 值语义下与逐次装箱等价。
	m.sched.Done(mh.boxed)
	if m.hostIdentity(host, mh) { // 二次墓碑检查（R4：指针同一性——同名重注册不 Rearm，新注册自有 Schedule 链；窗口宽度与 cbMu 无关，C3 前后等价）
		m.sched.Rearm(mh.boxed, pp.interval)
	}
	finished = true // S2：Done 与 Rearm 均完成后才置 true（C3：赋值点随步骤 5 移至 cbMu 外，单点赋值性质不变）
}

// hostParams 单次查找返回注册句柄、探测参数快照与存活标志（N4）：
// manager.mu.RLock 内捕获 *managedHost（R4 指针同一性锚点）并组装 probeParams
// （per-host cfg + 全局 rise/fall），ok=false 即墓碑（INV-3）。
// cfg 写入后不可变，快照跨锁使用安全。
func (m *HealthManager) hostParams(host string) (*managedHost, probeParams, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mh, ok := m.hosts[host]
	if !ok {
		return nil, probeParams{}, false
	}
	return mh, probeParams{
		checker:  mh.cfg.checker,
		timeout:  mh.cfg.timeout,
		interval: mh.cfg.interval,
		rise:     m.config.rise,
		fall:     m.config.fall,
	}, true
}

// getManagedHost 返回 host 的 managedHost 指针与存在性（RLock 查找，立即释放）。
func (m *HealthManager) getManagedHost(host string) (*managedHost, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mh, ok := m.hosts[host]
	return mh, ok
}

// hostIdentity 报告 hosts[host] 是否仍指向 captured（指针同一性即注册代语义，
// R4/L5 P2-2；按名判活无法区分"同一注册"与"RemoveHost→AddHost 同名重注册"）。
// captured==nil（步骤 1 墓碑路径）恒 false。manager.mu.RLock 内仅比对指针——
// 无新锁、锁序不变（持 cbMu 时调用合法：cbMu → manager.mu 单向）。
// 调用方：probeFn（主动路径 R4——步骤 3 复核在 cbMu 临界区内，P1-1 修复后与被动
// 路径对称；defer 收尾与步骤 5 的二次复核调用时不持 cbMu（C3），比对仍在
// manager.mu.RLock 保护下进行，窗口宽度与 cbMu 无关）。report 的被动路径复核同用
// 本函数（ERC Phase4 R1：失配一律无条件 Reset，不再需要区分失配形态，P1-3）。
func (m *HealthManager) hostIdentity(host string, captured *managedHost) bool {
	if captured == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hosts[host] == captured
}

// safeCheck 包裹一次 Checker.Check 调用（全库 recover 点 1/2：消费者 Checker 边界，
// docs/04 §4.3 逐行 + M-6 Timestamp 补填）。
//
// 返回值契约（docs/06 §1.2）：
//   - Checker panic → 合成 CheckResult{Healthy:false, Err:"checker panic: ..."}，
//     worker 不退出、其余 host 不受影响（高可用必需）；
//   - (nil, error) 基础设施错误 → 合成 CheckResult{Healthy:false, Err:err}；
//   - (nil, nil) → 视为 Healthy=false（契约已写明，非防御）；
//   - 正常路径若 Timestamp 为零值则以注入 Clock 补齐（M-6：发布前补填，
//     不违反 INV-4——尚未发布；Latency 耗时测量允许 time.Now，SF5 只约束 Timestamp）。
func (m *HealthManager) safeCheck(c checker.Checker, ctx context.Context) (result *checker.CheckResult) {
	defer func() {
		if r := recover(); r != nil { // 全库 recover 1/2：消费者 Checker 边界
			result = &checker.CheckResult{
				Healthy:   false,
				Err:       fmt.Errorf(panicErrPrefix+"%v", r),
				Timestamp: m.clock.Now(),
			}
		}
	}()
	res, err := c.Check(ctx)
	switch {
	case err != nil: // (nil,error) 基础设施错误 → 合成（docs/06 §1.2）
		res = &checker.CheckResult{Healthy: false, Err: err, Timestamp: m.clock.Now()}
	case res == nil: // (nil,nil) → 视为 Healthy=false（契约已写明，不做额外防御）
		res = &checker.CheckResult{Healthy: false, Timestamp: m.clock.Now()}
	default:
		if res.Timestamp.IsZero() {
			res.Timestamp = m.clock.Now() // M-6：发布前补填（INV-4 不违反——尚未发布）
		}
	}
	return res
}

// notifyLocked 投递状态翻转回调（全库 recover 点 2/2：消费者回调边界，docs/04 §4.4 逐行）。
//
// 前置条件：调用者已持有该 host 的 cbMu（sync.Mutex 非重入，本函数不再 Lock）；
// cbMu 在状态变更之前获取，翻转+快照+投递同一临界区（C1/INV-7），
// 且回调不在 HostState.mu 内（INV-5/R-e）。
//
// panic 政策：回调 panic 被隔离并路由到 WithOnInternalError sink（err 含
// "StateChangeCallback panic for host" 前缀）；sink 为 nil 时 panic 被隔离并
// 丢弃（文档化声明，docs/06 §1.3）。sink 在持 cbMu 期间被调用；sink 自身不被
// recover——它 panic 直接崩进程（fail-loud 终止规则）。
func (m *HealthManager) notifyLocked(host string, healthy bool, result *CheckResult) {
	if m.onChange == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil { // 全库 recover 2/2：消费者回调边界
			err := fmt.Errorf("StateChangeCallback panic for host %s: %v", host, r)
			if m.onInternalError != nil {
				m.onInternalError(host, err) // sink 在持 cbMu 期间被调用；sink 自身不被 recover——panic 直接崩进程（fail-loud 终止规则）
			}
			// sink 为 nil：panic 被隔离并丢弃（文档化声明，docs/06 §1.3）
		}
	}()
	m.onChange(host, healthy, result)
}
