// Package tcp 提供 TCP 健康检查 Checker 子包（纯标准库 net 实现）。
//
// 消费者以 blank import 完成 "tcp" 协议注册（对齐 database/sql 驱动模式）：
//
//	import _ "github.com/shengyanli1982/go-healthcheck/checker/tcp"
//
// 主 checker 包只保留 Checker 契约与 Registry；内置协议一个目录一个子包
// （对齐 checker/http、checker/grpc），根包与核心包不 import 协议子包，
// 依赖闭包保持最小（DoD #5 依赖审计门禁）。
package tcp

import (
	"context"
	"net"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
)

// TCPChecker 以 TCP 建连探测目标健康状态（纯标准库 net 实现）。
//
// 健康判定：建连成功即 Healthy=true；连接失败/超时即 Healthy=false。
// StatusCode 恒为 0（TCP 无状态码）。建连成功后立即关闭连接，不发送数据。
//
// 作为内置 Checker，失败路径统一事实化返回（M-5）：
// (*CheckResult{Healthy:false, Err:err}, nil)，不占用 (nil, error) 通道。
type TCPChecker struct {
	addr    string
	timeout time.Duration
}

// Check 执行一次 TCP 建连探测并返回事实化结果。
// 尊重 ctx 取消/超时；Latency 为建连耗时；
// 结果不填探测时刻字段（M-6，由根包 safeCheck 以注入 Clock 补齐）。
func (c *TCPChecker) Check(ctx context.Context) (*checker.CheckResult, error) {
	start := time.Now()
	d := net.Dialer{Timeout: c.timeout}
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return &checker.CheckResult{Healthy: false, StatusCode: 0, Latency: time.Since(start), Err: err}, nil
	}
	_ = conn.Close()
	return &checker.CheckResult{Healthy: true, StatusCode: 0, Latency: time.Since(start)}, nil
}

// newTCPChecker 是 "tcp" 协议的 CheckerFactory。
func newTCPChecker(cfg checker.TargetConfig) (checker.Checker, error) {
	if cfg.Addr == "" {
		return nil, checker.ErrEmptyTargetAddress // 共享 sentinel 直返（消息自包含 "checker: " 前缀，docs/06 §5）
	}
	return &TCPChecker{addr: cfg.Addr, timeout: cfg.Timeout}, nil
}

func init() {
	// blank import 触发注册（对齐 database/sql 驱动模式）。
	// 协议名取 checker.ProtocolTCP（注册名单一事实源，固定非空）、factory 非 nil，
	// Register 不会失败（零日志，丢弃返回值）。
	_ = checker.Register(checker.ProtocolTCP, newTCPChecker)
}
