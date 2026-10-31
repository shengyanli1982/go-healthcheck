package checker

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]CheckerFactory)
)

// Registry 导出 sentinel 错误（消息自包含 "checker: " 前缀，即最终消息；
// 无上下文增量的返回点直返，wrap 仅以统一后缀模板 "%w (protocol %q)"
// 附加协议名增量——设计原则见 docs/06 §5，消费者经 errors.Is 判别）：
var (
	// ErrEmptyProtocol 由 Register 在 protocol 为空字符串时返回（直返；
	// protocol 为空无可附加上下文）。
	ErrEmptyProtocol = errors.New("checker: empty protocol name")

	// ErrNilFactory 由 Register 在 factory 为 nil 时返回
	// （返回点以 "%w (protocol %q)" 附加协议名上下文）。
	ErrNilFactory = errors.New("checker: nil factory")

	// ErrProtocolNotRegistered 由 NewChecker 在协议未注册时返回（不 panic；
	// 返回点以 "%w (protocol %q)" 附加协议名上下文）。
	ErrProtocolNotRegistered = errors.New("checker: protocol not registered")
)

// 内置协议注册名的单一事实源（untyped string 常量）：checker/http、
// checker/tcp、checker/grpc 三子包 init() 即以下列常量注册；消费者经
// WithProtocol / NewChecker 使用内置协议时优先传这些常量，避免手写字面量
// 拼写错误。自定义协议不受此约束——Register 与 WithProtocol 仍接受任意
// string（扩展机制见 docs/09-extensibility.md）。
const (
	// ProtocolHTTP 是内置 HTTP 协议的注册名（checker/http 子包 init() 注册）。
	ProtocolHTTP = "http"

	// ProtocolTCP 是内置 TCP 协议的注册名（checker/tcp 子包 init() 注册）。
	ProtocolTCP = "tcp"

	// ProtocolGRPC 是内置 gRPC 协议的注册名（checker/grpc 子包 init() 注册）。
	ProtocolGRPC = "grpc"
)

// Register 将协议名与 CheckerFactory 关联。
// 通常在 init() 中调用（类比 database/sql.Register、image.RegisterFormat）。
// 并发安全（内部 sync.RWMutex）。
// protocol 为空或 factory 为 nil 时返回 error。
// 重复注册同一协议名：后注册覆盖前注册（无日志输出）。
func Register(protocol string, factory CheckerFactory) error {
	if protocol == "" {
		return ErrEmptyProtocol // sentinel 直返（消息自包含前缀，docs/06 §5）
	}
	if factory == nil {
		return fmt.Errorf("%w (protocol %q)", ErrNilFactory, protocol) // 协议名为上下文增量
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[protocol] = factory
	return nil
}

// NewChecker 按协议名创建 Checker 实例。
// 协议未注册时返回 error（不 panic）。
// factory 返回的 error 原样透传。
func NewChecker(protocol string, cfg TargetConfig) (Checker, error) {
	registryMu.RLock()
	factory, ok := registry[protocol]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w (protocol %q)", ErrProtocolNotRegistered, protocol) // 协议名为上下文增量
	}
	return factory(cfg)
}

// Protocols 返回所有已注册协议名（字典序排序，确定性输出）。
func Protocols() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
