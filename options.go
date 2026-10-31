package healthcheck

import (
	"fmt"
	"runtime"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
	"github.com/shengyanli1982/go-healthcheck/passive"
)

// CheckResult 是 checker.CheckResult 的类型别名（根包重导出，M-2）。
// 消费者仅 import 根包即可使用 StateChangeCallback / GetLastResult（docs/11 M1）。
// 字段语义与不可变性约束（R-d）见 checker.CheckResult 的 godoc。
type CheckResult = checker.CheckResult

// StateChangeCallback 状态变化回调函数类型。
// 携带触发变化的最近 CheckResult（被动翻转时 result 为 nil）。
//
// 调用约束（R-e）：必须在 HostState.mu 之外调用。
// 原因：锁内调用消费者代码 = 死锁与不可预测延迟的经典来源。
//
// 并发契约（P0-1）：回调可能被多个 goroutine 并发调用（不同 host 的翻转互不串行）。
// 消费者实现必须并发安全。
//
// 顺序契约（F-b）：同一 host 的状态翻转事件保证串行投递
// （cbMu 在状态变更之前获取，翻转+快照+投递在同一临界区）。
// 主动路径严格按发生序；被动路径按 cbMu 获取序（detector 判定与 cbMu 获取
// 之间可因慢回调出现窗口）。内置 TimeWindowDetector 实现可选复核接口
// passive.CurrentHealthyReader：被动投递前在 cbMu 临界区内复核 detector
// 当前值，被并发翻转抢先的 stale 快照不再覆盖镜像——投递流静止后镜像与
// detector 收敛，回调 healthy 参数与 IsHealthy 终态一致。未实现该接口的
// 自定义 detector 退化为旧语义（直接投递 Record 快照），慢回调场景可产生
// 分歧窗口，由消费者自行承担。
// 不同 host 之间无顺序保证。
//
// 翻转语义（P3-2 文档化裁定——两条路径的"翻转"判据不对称，属设计内行为）：
//   - 主动路径事件 = 主动状态机翻转（ActiveHealthy 翻转，不含被动镜像）：
//     host 被动 DOWN 期间主动状态机翻转（如 flapping）也会产生事件，其间综合
//     状态可能从未变化——事件流不保证 true/false 交替，消费者不得假设交替；
//   - 被动路径事件 = 综合翻转（IsHealthy 值变化）；
//   - healthy 参数恒为投递时的综合状态快照（ActiveHealthy && PassiveHealthy），
//     与回调内重读 IsHealthy(host) 读同一镜像（N1 自洽前提）。
//
// 参数语义：healthy 是锁内读取的快照值（ApplyResult / SetPassiveHealthy 返回的
// 锁内快照，与翻转写入同一临界区产出），非锁外重读。该值是翻转当时的快照；
// 被动路径存在极窄的判定/投递窗口，维护强一致缓存的消费者可在回调内重读
// IsHealthy(host)。result 指向不可变对象（R-d），被动翻转时为 nil。
//
// 回调在哪个 goroutine 执行：probe-worker（主动翻转）或宿主请求 goroutine（被动翻转）。
//
// 回调禁区（违反 = 死锁或未定义行为）：
//
//	MUST NOT 调用：Start, Stop, AddHost, RemoveHost, ReportSuccess, ReportFailure
//	- Stop：Shutdown 立即关闭 + 有界等待 worker 退出 → 自死锁
//	- AddHost/RemoveHost：持有 manager.mu → 死锁
//	- ReportSuccess/ReportFailure：非重入 cbMu 自死锁 → 收尾永不执行 → Stop 有界等待超时返回 error（R3；泄漏 worker 由宿主处置）
//	- Start：重复启动未定义行为
//	- 禁止无限阻塞（会拖慢同 host 的下一次回调 + 占用 probe-worker）
//	- 必须快速返回
//
// 回调白名单（允许在回调内调用）：
//
//	IsHealthy, GetHealthyHosts, GetLastResult, Hosts
//
// panic 隔离：回调 panic 被库 recover（notifyLocked），路由到 WithOnInternalError。
// 宿主进程不受影响，worker 池存活。
type StateChangeCallback func(host string, healthy bool, result *CheckResult)

// InternalErrorHandler 承载库隔离消费者回调 panic 后的通知；唯一载荷 = 被 notifyLocked recover 的 StateChangeCallback panic。
// 默认 nil = panic 被隔离并丢弃（宿主进程不受影响，worker 池存活）。
// 消费者可注入以自行 log/告警/metric。
//
// 调用发生在与触发它的 StateChangeCallback 同一 goroutine：
// probe-worker（主动翻转）或宿主请求 goroutine（被动翻转）。
// 实现 MUST 快速返回、MUST NOT 阻塞。
// 禁止调用 Start/Stop/AddHost/RemoveHost/ReportSuccess/ReportFailure（同 StateChangeCallback 禁区）。
// 注意：sink 在持该 host cbMu 期间被调用。
//
// 终止规则：本 handler 自身不被 recover。若它 panic，直接崩溃进程（fail-loud）。
// 它是最后手段，坏了就该炸。
type InternalErrorHandler func(host string, err error)

// managerConfig 承载全局配置（12 个 Option 的落点，docs/05 §2）。
// 不导出；仅经 Option 修改、经 validate 校验（New() 构造时一次性 fail-fast）。
type managerConfig struct {
	interval           time.Duration           // 探测间隔，默认 5s，有效范围 (0, 24h]
	timeout            time.Duration           // 单次探测超时，默认 2s，有效范围 (0, 24h]
	rise               int                     // UP 所需连续成功次数，默认 2，有效范围 [1, 1000]
	fall               int                     // DOWN 所需连续失败次数，默认 3，有效范围 [1, 1000]
	workers            int                     // 调度 worker 数，默认 runtime.NumCPU()，有效范围 [1, 4096]
	clock              Clock                   // 时间来源，默认 realClock{}；显式 nil 非法
	spreadSlots        int                     // 确定性错峰槽位数，默认 16，有效范围 [1, 4096] 且 interval/n ≥ 1ms
	onChange           StateChangeCallback     // 状态变化回调，默认 nil（禁用）；nil 合法
	passiveDetector    passive.PassiveDetector // nil 且未显式设置 → ST-07 New() 构造默认 TimeWindowDetector
	passiveDetectorSet bool                    // Want-Shift R2：WithPassiveDetector 被显式调用过（validate 据此区分：显式 nil=error / 默认 nil=合法）
	passiveMaxFails    int                     // 被动窗口内最大失败次数，默认 3，有效范围 [1, 1000]
	passiveFailTimeout time.Duration           // 被动时间窗口，默认 10s，有效范围 (0, 24h]
	onInternalError    InternalErrorHandler    // 回调 panic 隔离通知 sink，默认 nil（隔离丢弃）；nil 合法
}

// newDefaultConfig 返回全默认值配置（docs/05 §2/§4 默认值全表）。
// 零配置即可生产使用；返回的配置必然通过 validate。
func newDefaultConfig() *managerConfig {
	return &managerConfig{
		interval:           5 * time.Second,
		timeout:            2 * time.Second,
		rise:               2,
		fall:               3,
		workers:            runtime.NumCPU(),
		clock:              realClock{},
		spreadSlots:        16,
		passiveMaxFails:    3,
		passiveFailTimeout: 10 * time.Second,
	}
}

// Option 是 HealthManager 的全局配置选项。
type Option func(*managerConfig)

// WithInterval 设置全局探测间隔（固定延迟语义：周期 = interval + 探测耗时）。
// 有效范围：0 < d ≤ 24h，即 (0, 24h]；非法值 New() 返回 error（fail-fast）。
// 默认值：5s。最佳实践：timeout < interval（建议非强制，不校验）。
// 组合约束：interval/spreadSlots ≥ 1ms（见 WithSpreadSlots）。
func WithInterval(d time.Duration) Option { return func(c *managerConfig) { c.interval = d } }

// WithTimeout 设置单次探测超时（per-probe context.WithTimeout）。
// 有效范围：0 < d ≤ 24h，即 (0, 24h]；非法值 New() 返回 error（fail-fast）。
// 默认值：2s。timeout > interval 组合合法（建议非强制，docs/05 §5）。
func WithTimeout(d time.Duration) Option { return func(c *managerConfig) { c.timeout = d } }

// WithRise 设置 UP 所需连续成功次数（对齐 nginx rise）。
// 有效范围：1 ≤ n ≤ 1000；非法值 New() 返回 error（fail-fast）。默认值：2。
func WithRise(n int) Option { return func(c *managerConfig) { c.rise = n } }

// WithFall 设置 DOWN 所需连续失败次数（对齐 nginx fall）。
// 有效范围：1 ≤ n ≤ 1000；非法值 New() 返回 error（fail-fast）。默认值：3。
func WithFall(n int) Option { return func(c *managerConfig) { c.fall = n } }

// WithWorkers 设置调度 worker 数（探测并发上限）。
// 有效范围：1 ≤ n ≤ 4096；非法值 New() 返回 error（fail-fast）。
// 默认值：runtime.NumCPU()（探测是 I/O 密集，NumCPU 足够）。
func WithWorkers(n int) Option { return func(c *managerConfig) { c.workers = n } }

// WithClock 注入自定义时钟（测试用 fakeClock 控制时间流速）。
// 有效范围：显式传 nil → New() 返回 error（fail-fast）；默认值：realClock{}。
// Clock 注入覆盖范围（SF5）：CheckResult.Timestamp、被动时间窗口；
// 不覆盖 workqueue/v2 TimerQueue 的调度触发时刻。
func WithClock(c Clock) Option { return func(cfg *managerConfig) { cfg.clock = c } }

// WithSpreadSlots 设置确定性错峰槽位数（非随机，确定性输出）。
// 有效范围：1 ≤ n ≤ 4096，且 interval/n ≥ 1ms（R-b，TimerQueue 毫秒精度；
// New() validate 硬校验，等值合法——判定式 interval ≥ spreadSlots×1ms）。
// 默认值：16（5s/16=312.5ms 粒度，足够均匀且远超 1ms 精度限制）。
func WithSpreadSlots(n int) Option { return func(c *managerConfig) { c.spreadSlots = n } }

// WithOnStateChange 设置状态变化回调（翻转时触发）。
// 有效范围：nil 合法（= 默认禁用，与默认值一致）。
// 非 nil 时的并发契约（P0-1）、顺序契约（F-b）、回调禁区与白名单
// 见 StateChangeCallback 的 godoc。
func WithOnStateChange(cb StateChangeCallback) Option {
	return func(c *managerConfig) { c.onChange = cb }
}

// WithPassiveDetector 注入自定义被动检测器。
// 有效范围：显式传 nil → New() 返回 error（fail-fast）；
// 默认（未调用本 Option）时 passiveDetector 为 nil 且合法，
// New() 以 passiveMaxFails/passiveFailTimeout 构造默认 TimeWindowDetector。
// 应用本 Option 会置 passiveDetectorSet=true，validate 据此区分显式 nil 与默认 nil。
func WithPassiveDetector(d passive.PassiveDetector) Option {
	return func(c *managerConfig) {
		c.passiveDetector = d
		c.passiveDetectorSet = true
	}
}

// WithPassiveMaxFails 设置被动检测：窗口内最大失败次数（nginx max_fails）。
// 有效范围：1 ≤ n ≤ 1000；非法值 New() 返回 error（fail-fast）。默认值：3。
func WithPassiveMaxFails(n int) Option { return func(c *managerConfig) { c.passiveMaxFails = n } }

// WithPassiveFailTimeout 设置被动检测：时间窗口（nginx fail_timeout）。
// 有效范围：0 < d ≤ 24h，即 (0, 24h]；非法值 New() 返回 error（fail-fast）。默认值：10s。
func WithPassiveFailTimeout(d time.Duration) Option {
	return func(c *managerConfig) { c.passiveFailTimeout = d }
}

// WithOnInternalError 设置库隔离消费者回调 panic 后的通知 sink
// （唯一载荷 = 被 notifyLocked recover 的 StateChangeCallback panic）。
// 有效范围：nil 合法（= 默认禁用：panic 被隔离并丢弃）。
// 非 nil 时的调用 goroutine、禁区、终止规则（fail-loud）见 InternalErrorHandler 的 godoc。
func WithOnInternalError(fn InternalErrorHandler) Option {
	return func(c *managerConfig) { c.onInternalError = fn }
}

// hostConfig 承载 per-host 覆盖配置（4 个 HostOption 的落点，docs/05 §3）。
// 不导出；AddHost 应用 HostOption 后先 validate(host)（范围校验），
// 再解析零值哨兵为全局值（ST-07）。
type hostConfig struct {
	checker  checker.Checker // WithChecker 直传实例（优先于 protocol）
	protocol string          // WithProtocol 协议名（Registry 创建）
	interval time.Duration   // 0 = 未覆盖，AddHost 时解析为全局值
	timeout  time.Duration   // 0 = 未覆盖，同上
}

// HostOption 是单个主机的配置选项（per-host 覆盖）。
type HostOption func(*hostConfig)

// WithChecker 直接传入 Checker 实例（跳过 Registry）。
// 优先级：WithChecker > WithProtocol（互斥，Checker 优先，docs/05 §3）。
// AddHost 必须指定 WithChecker 或 WithProtocol 之一（都不选返回 error）。
func WithChecker(c checker.Checker) HostOption { return func(h *hostConfig) { h.checker = c } }

// WithProtocol 按协议名从 Registry 创建 Checker。
// 优先级：WithChecker > WithProtocol（互斥，Checker 优先）；
// 协议未注册时 AddHost 返回 error（不 panic）。
// 需 blank import 对应协议子包（checker/http、checker/tcp、checker/grpc）
// 完成注册（database/sql 驱动模式，见根包文档）。
// 内置协议建议传 checker.ProtocolHTTP / ProtocolTCP / ProtocolGRPC 常量
// （注册名单一事实源，避免手写字面量拼写错误）；自定义协议仍可传任意 string
// （扩展机制见 docs/09-extensibility.md）。
// 限制：AddHost 经本 Option 构造的 TargetConfig.Extra 恒为 nil（v1 不提供 WithExtra），
// 协议特定配置（如 http path/expected_status、grpc service/tls）不可达；
// 需要时先以 checker.NewChecker(protocol, checker.TargetConfig{...}) 构造 Checker，
// 再经 WithChecker 传入（协议特定配置的唯一通路，docs/05 §7）。
func WithProtocol(name string) HostOption { return func(h *hostConfig) { h.protocol = name } }

// WithHostInterval 覆盖全局探测间隔（per-host；WithHostInterval > WithInterval）。
// 有效范围：零值 = 未覆盖（哨兵语义，AddHost 时解析为全局值，合法）；
// 显式设置时 0 < d ≤ 24h（同全局 interval）；负值或超上限 AddHost 返回 error。
func WithHostInterval(d time.Duration) HostOption { return func(h *hostConfig) { h.interval = d } }

// WithHostTimeout 覆盖全局探测超时（per-host；WithHostTimeout > WithTimeout）。
// 有效范围：零值 = 未覆盖（哨兵语义，AddHost 时解析为全局值，合法）；
// 显式设置时 0 < d ≤ 24h（同全局 timeout）；负值或超上限 AddHost 返回 error。
func WithHostTimeout(d time.Duration) HostOption { return func(h *hostConfig) { h.timeout = d } }

// —— 校验上限常量（Want-Shift R2；未导出——不新增任何导出 API，
// API 面积审计"恰 12 Option + 4 HostOption"不变，docs/05 §5）——
const (
	maxValidInterval           = 24 * time.Hour // WithInterval / WithHostInterval 上限
	maxValidTimeout            = 24 * time.Hour // WithTimeout / WithHostTimeout 上限
	maxValidRise               = 1000
	maxValidFall               = 1000
	maxValidWorkers            = 4096
	maxValidSpreadSlots        = 4096
	maxValidPassiveMaxFails    = 1000
	maxValidPassiveFailTimeout = 24 * time.Hour
)

// validate 校验全局配置（docs/05 §5 修订版全矩阵：下限 + 上限 + 毫秒精度 + nil 注入）。
// fail-fast：返回 error，不 panic、不静默钳制或回退默认值；New() 构造时调用一次。
// 错误为导出 sentinel 直返（消息自包含 "healthcheck: " 前缀，errors.Is 可判别，
// docs/06 §5；全局级无上下文增量，不做纯前缀包装）。
// 校验顺序：先 interval 合法，再校验 spreadSlots 毫秒精度（后者依赖前者）。
// timeout < interval 仅 godoc 建议，不强制；onChange/onInternalError 为 nil 合法（默认禁用）。
func (c *managerConfig) validate() error {
	if c.interval <= 0 || c.interval > maxValidInterval {
		return ErrInvalidInterval
	}
	if c.timeout <= 0 || c.timeout > maxValidTimeout {
		return ErrInvalidTimeout
	}
	if c.rise < 1 || c.rise > maxValidRise {
		return ErrInvalidRise
	}
	if c.fall < 1 || c.fall > maxValidFall {
		return ErrInvalidFall
	}
	if c.workers < 1 || c.workers > maxValidWorkers {
		return ErrInvalidWorkers
	}
	if c.spreadSlots < 1 || c.spreadSlots > maxValidSpreadSlots {
		return ErrInvalidSpreadSlots
	}
	// 毫秒精度组合约束（R-b）：interval/spreadSlots ≥ 1ms，等值合法；依赖 interval 已通过范围校验
	if c.interval < time.Duration(c.spreadSlots)*time.Millisecond {
		return ErrInvalidSpreadPrecision
	}
	if c.passiveMaxFails < 1 || c.passiveMaxFails > maxValidPassiveMaxFails {
		return ErrInvalidPassiveMaxFails
	}
	if c.passiveFailTimeout <= 0 || c.passiveFailTimeout > maxValidPassiveFailTimeout {
		return ErrInvalidPassiveFailTimeout
	}
	// cfg.clock==nil 只可能来自 WithClock(nil)——默认 realClock 非 nil（Want-Shift R2 fail-fast）
	if c.clock == nil {
		return ErrNilClock
	}
	// 仅当 WithPassiveDetector 被显式调用且传入 nil 时报错；
	// 默认 nil 合法（ST-07 New() 构造默认 TimeWindowDetector）
	if c.passiveDetectorSet && c.passiveDetector == nil {
		return ErrNilPassiveDetector
	}
	return nil
}

// validate 校验 host 级显式覆盖范围（docs/05 §5；ST-07 AddHost 在应用
// HostOption 后、解析全局值前调用）。零值 = 未覆盖哨兵，合法；
// 负值或超上限返回 error——复用全局 sentinel（ErrInvalidInterval/ErrInvalidTimeout），
// host 名是真实上下文增量，以全库统一后缀模板 "%w (host %q)" 附加
// （sentinel 消息自包含前缀仍在句首，见 docs/06 §5）。
func (c *hostConfig) validate(host string) error {
	if c.interval < 0 || c.interval > maxValidInterval {
		return fmt.Errorf("%w (host %q)", ErrInvalidInterval, host)
	}
	if c.timeout < 0 || c.timeout > maxValidTimeout {
		return fmt.Errorf("%w (host %q)", ErrInvalidTimeout, host)
	}
	return nil
}
