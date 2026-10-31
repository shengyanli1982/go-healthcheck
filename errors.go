package healthcheck

import "errors"

// 根包导出 sentinel 错误全集（权威清单与设计原则见 docs/06 §5）。
//
// 设计原则（2026-10-29 用户纠正修订：sentinel 自包含前缀 + 直返）：
//   - sentinel 消息自带完整包前缀 "healthcheck: "，即最终消息（对齐 stdlib
//     http.ErrServerClosed = "http: Server closed" 惯例）；
//   - 无上下文增量的返回点直接 return sentinel，禁止
//     fmt.Errorf("healthcheck: %w", ErrXxx) 纯前缀包装（冗余）；
//   - fmt.Errorf + %w 仅用于真实上下文增量，全库统一后缀模板
//     fmt.Errorf("%w (host %q)", ErrXxx, host)——包前缀仍在句首，
//     括号内附加运维定位键值（如 host 名）；
//   - 导出判据：消费者可能 errors.Is 判别即导出（如
//     scheduler.ErrNotBlockingQueue，依赖升级 bug 定位场景）；
//   - 动态内容错误（panic 包装、HTTP status code、gRPC status）保持
//     fmt.Errorf 合成，不提炼 sentinel（不过度设计）。
//
// 消费者经 errors.Is 稳定判别（直返与 %w 后缀包装均保持 sentinel 身份）：
//
//	if errors.Is(err, healthcheck.ErrHostExists) { ... }
var (
	// ErrAlreadyStarted 由 Start 重复启动时返回（生命周期校验，fail-fast；直返）。
	ErrAlreadyStarted = errors.New("healthcheck: already started")

	// ErrStopped 由 Stop 之后的 Start/AddHost 返回（生命周期一次性，L5 P2-2a；直返）。
	ErrStopped = errors.New("healthcheck: manager stopped")

	// ErrEmptyHost 由 AddHost 在 host 为空字符串时返回（唯一参数校验点；直返）。
	ErrEmptyHost = errors.New("healthcheck: empty host")

	// ErrHostExists 由 AddHost 重复注册同名 host 时返回
	// （返回点以 "%w (host %q)" 附加 host 名上下文）。
	ErrHostExists = errors.New("healthcheck: host already exists")

	// ErrHostNotFound 由 RemoveHost 在 host 不存在时返回
	// （返回点以 "%w (host %q)" 附加 host 名上下文）。
	ErrHostNotFound = errors.New("healthcheck: host not found")

	// ErrNoChecker 由 AddHost 在 WithChecker 与 WithProtocol 均未指定时返回
	// （返回点以 "%w (host %q)" 附加 host 名上下文）。
	ErrNoChecker = errors.New("healthcheck: neither checker nor protocol specified")

	// ErrNilClock 由 New 在显式注入 WithClock(nil) 时返回（fail-fast；直返；
	// 默认 realClock 非 nil，不调用该 Option 合法）。
	ErrNilClock = errors.New("healthcheck: clock must not be nil")

	// ErrNilPassiveDetector 由 New 在显式注入 WithPassiveDetector(nil) 时返回
	// （fail-fast；直返；默认 nil 合法——不调用该 Option 时 New 构造默认 TimeWindowDetector）。
	ErrNilPassiveDetector = errors.New("healthcheck: passiveDetector must not be nil")

	// ErrInvalidInterval 由 New（全局 WithInterval）或 AddHost（host 级
	// WithHostInterval）在 interval 超出 (0, 24h] 时返回；两级复用同一 sentinel：
	// 全局级直返，host 级以 "%w (host %q)" 附加 host 名上下文。
	// host 级零值为未覆盖哨兵，合法。
	ErrInvalidInterval = errors.New("healthcheck: interval must be in (0, 24h]")

	// ErrInvalidTimeout 由 New（全局 WithTimeout）或 AddHost（host 级
	// WithHostTimeout）在 timeout 超出 (0, 24h] 时返回；两级复用同一 sentinel
	// （同 ErrInvalidInterval）。host 级零值为未覆盖哨兵，合法。
	ErrInvalidTimeout = errors.New("healthcheck: timeout must be in (0, 24h]")

	// ErrInvalidRise 由 New 在 rise 超出 [1, 1000] 时返回（直返）。
	ErrInvalidRise = errors.New("healthcheck: rise must be in [1, 1000]")

	// ErrInvalidFall 由 New 在 fall 超出 [1, 1000] 时返回（直返）。
	ErrInvalidFall = errors.New("healthcheck: fall must be in [1, 1000]")

	// ErrInvalidWorkers 由 New 在 workers 超出 [1, 4096] 时返回（直返）。
	ErrInvalidWorkers = errors.New("healthcheck: workers must be in [1, 4096]")

	// ErrInvalidSpreadSlots 由 New 在 spreadSlots 超出 [1, 4096] 时返回（直返）。
	ErrInvalidSpreadSlots = errors.New("healthcheck: spreadSlots must be in [1, 4096]")

	// ErrInvalidSpreadPrecision 由 New 在 interval/spreadSlots < 1ms 时返回
	// （R-b 毫秒精度组合约束，等值合法；依赖 interval 已通过范围校验；直返）。
	ErrInvalidSpreadPrecision = errors.New("healthcheck: interval/spreadSlots must be >= 1ms")

	// ErrInvalidPassiveMaxFails 由 New 在 passiveMaxFails 超出 [1, 1000] 时返回（直返）。
	ErrInvalidPassiveMaxFails = errors.New("healthcheck: passiveMaxFails must be in [1, 1000]")

	// ErrInvalidPassiveFailTimeout 由 New 在 passiveFailTimeout 超出 (0, 24h] 时返回（直返）。
	ErrInvalidPassiveFailTimeout = errors.New("healthcheck: passiveFailTimeout must be in (0, 24h]")
)

// panicErrPrefix 是 safeCheck recover Checker panic 后合成 CheckResult.Err 的固定前缀
// （docs/06 §2.2 recover 点 (1)）。panic 包装属动态内容错误，不提炼 sentinel；
// 但前缀本身是文档化契约（集成测试断言），提炼为常量防止散落。
const panicErrPrefix = "checker panic: "
