// Package checker 定义主动健康探测的 Checker 行为契约与协议 Registry，
// 不含具体协议实现。
//
// 协议扩展：实现 Checker 接口后经 Register 注册协议名，即可被
// NewChecker 按名构造；核心（state/passive/scheduler）对具体协议零感知。
// 内置协议 "http"、"tcp"、"grpc" 位于独立子包（一个协议一个目录：
// checker/http、checker/tcp、checker/grpc），消费者 blank import 对应
// 子包即完成注册（对齐 database/sql 驱动模式）。
package checker

import (
	"context"
	"errors"
	"time"
)

// ErrEmptyTargetAddress 由内置协议工厂在 TargetConfig.Addr 为空时返回
// （构造期失败，不进入探测）。消息自包含 "checker: " 前缀（docs/06 §5）；
// http/tcp/grpc 三子包统一直返本 sentinel（共享 sentinel 不造子包前缀变体）。
// 消费者可经 errors.Is(err, checker.ErrEmptyTargetAddress) 判别。
var ErrEmptyTargetAddress = errors.New("checker: empty target address")

// CheckResult 承载单次探测的完整结果。
// 设计哲学：机制/策略分离 -- 库产出事实，消费者按自己策略解读。
// 字段精简原则：每个字段都有明确的消费者用途，不做 kitchen-sink。
//
// 不可变性约束（R-d）：每轮探测必须新建 CheckResult，发布后不可变。
// 禁止原地修改已发布的 CheckResult，禁止返回内部可变指针。
type CheckResult struct {
	// Healthy 本次探测的健康判定（true=成功，false=失败）。
	// 用途：rise/fall 状态机的输入信号；消费者自定义逻辑的判断依据。
	Healthy bool

	// StatusCode 协议相关的状态码（HTTP status / gRPC codes.Code）。
	// 零值表示不适用（如 TCP 探测无状态码）。
	// 用途：消费者可按状态码做细粒度路由决策（如 503 vs 502 区分处理）。
	StatusCode int

	// Latency 本次探测耗时。
	// 用途：慢节点识别、P99 观测、告警、超时预算调优。
	// 注意：这是探测延迟，不等于真实请求平均延迟，不应直接喂给 LeastTime 选择器。
	Latency time.Duration

	// Err 探测失败的原因（Healthy=true 时为 nil）。
	// 用途：日志/告警/调试；消费者可根据 error 类型做差异化处理。
	Err error

	// Timestamp 探测完成时刻（来自注入的 Clock）。
	// 用途：消费者判断结果新鲜度（staleness detection）；可复现性保证。
	// M-6：内置 Checker 保持零值，由根包 safeCheck 以注入 Clock 补齐。
	Timestamp time.Time
}

// Checker 定义主动健康探测的行为契约。
// 设计模式：Strategy（协议探测算法可替换）。
// 推导来源：硬约束 #6（协议扩展性）。
//
// 返回值契约：
//   - (*CheckResult, nil) -- 探测正常执行完毕，结果在 CheckResult 中
//   - (nil, error)        -- 探测器自身无法执行（基础设施错误），库视为探测失败
//
// 选择双返回值的理由（类比 net/http.RoundTripper）：
//  1. Go 惯例；
//  2. 区分"探测到目标不健康"与"探测器自身出错"两种语义；
//  3. error != nil 时库自动构造 Healthy=false 的内部结果，Checker 无需处理此情况。
//
// 实现约束：
//   - Check 必须尊重 ctx 取消/超时（context.Done()）
//   - Check 禁止修改传入的 ctx
//   - 返回的 *CheckResult 一旦返回即不可变
//   - 同一 Checker 实例可被多个 worker 并发调用（多 host 共享场景，如
//     WithChecker 传入同一实例注册多个 host），实现必须并发安全；
//     内置 http/tcp/grpc checker 均无状态，天然满足（2026-10-30 审计 P3-6）
type Checker interface {
	Check(ctx context.Context) (*CheckResult, error)
}

// CheckerFactory 创建特定协议的 Checker 实例。
// 返回 error 表示配置不合法（如地址格式错误）。
type CheckerFactory func(cfg TargetConfig) (Checker, error)

// TargetConfig 传递给 CheckerFactory 的目标主机配置。
type TargetConfig struct {
	Addr    string            // 目标地址 (host:port)
	Timeout time.Duration     // 单次探测超时
	Extra   map[string]string // 协议特定配置（如 HTTP path、gRPC service name）
}
