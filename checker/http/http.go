// Package http 提供 HTTP 健康检查 Checker 子包（纯标准库 net/http 实现）。
//
// 消费者以 blank import 完成 "http" 协议注册（对齐 database/sql 驱动模式）：
//
//	import _ "github.com/shengyanli1982/go-healthcheck/checker/http"
//
// 主 checker 包只保留 Checker 契约与 Registry；内置协议一个目录一个子包
// （对齐 checker/tcp、checker/grpc），根包与核心包不 import 协议子包，
// 依赖闭包保持最小（DoD #5 依赖审计门禁）。
package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"strconv"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
)

// maxDrainBytes 是 Close 前对响应 body 的有界 drain 字节上限。
// 4096 覆盖健康检查 body 典型尺寸上界（"OK"、短 JSON 等）；至多读 4KB 后即
// Close（超限部分不再排空、不追求读至 EOF，该连接弃用不复用），
// 防超大/恶意 body 造成无界读放大。
const maxDrainBytes = 4096

// ErrInvalidExpectedStatus 由 "http" 协议工厂在 Extra["expected_status"]
// 含非法状态码 token（非数字）或越界数值（合法口径 [100,599]，2026-10-29 审计 P2-2 修复；
// 显式 "0"/负值同样拒绝——与 "0=未设置" 哨兵冲突）时返回（构造期失败，
// 不进入探测）。依 docs/07 §5 安全约束，TargetConfig.Extra 值不进入 error
// 消息（可能含密码/secret），故本 sentinel 不回显具体值，返回点也不 wrap
// strconv 解析错误（其消息本身含原值）。
// 消息自包含 "checker: " 前缀（docs/06 §5，不造子包前缀变体）；
// 消费者可经 errors.Is(err, http.ErrInvalidExpectedStatus) 判别。
var ErrInvalidExpectedStatus = errors.New("checker: invalid expected_status")

// HTTPChecker 以 HTTP 协议探测目标健康状态（纯标准库 net/http 实现）。
//
// 健康判定（G4）：未指定 Extra["expected_status"] 时，响应状态码位于
// [200, 399] 判为健康；指定时按该状态码精确匹配。
//
// 请求经 net/http.DefaultTransport.RoundTrip 发送：复用连接池，且不跟随重定向
// （3xx 响应原样返回并参与默认区间判定）。默认 path 为 "/health"，
// 默认 method 为 "GET"，均可经 Extra 覆盖。
//
// 作为内置 Checker，失败路径统一事实化返回（M-5）：
// (*CheckResult{Healthy:false, Err:err}, nil)，不占用 (nil, error) 通道。
type HTTPChecker struct {
	addr           string
	path           string        // 默认 "/health"
	method         string        // 默认 "GET"
	timeout        time.Duration // >0 时自包 context.WithTimeout（仅服务 standalone 使用场景——HTTPChecker 为导出类型，消费者可脱离 manager 单独使用；库路径恒被外层 probeCtx 先触发：两者时长恒等且内层创建更晚，deadline 严格更晚，OE-1）
	expectedStatus int           // 0 = 默认区间判定 200-399（G4）
}

// Check 发送一次 HTTP 探测并返回事实化结果。
// 尊重 ctx 取消/超时；Latency 为发起请求到收到响应头的耗时；
// 结果不填探测时刻字段（M-6，由根包 safeCheck 以注入 Clock 补齐）。
func (c *HTTPChecker) Check(ctx context.Context) (*checker.CheckResult, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout) // standalone 语义依赖，禁止删除（OE-1 裁定）
		defer cancel()
	}
	start := time.Now()
	req, err := nethttp.NewRequestWithContext(ctx, c.method, "http://"+c.addr+c.path, nil)
	if err != nil {
		return &checker.CheckResult{Healthy: false, StatusCode: 0, Latency: time.Since(start), Err: err}, nil
	}
	resp, err := nethttp.DefaultTransport.RoundTrip(req)
	if err != nil {
		return &checker.CheckResult{Healthy: false, StatusCode: 0, Latency: time.Since(start), Err: err}, nil
	}
	// 有界 drain 后 Close（2026-10-30 审计 P2-2 修复）：body 读到 EOF 连接才归还 DefaultTransport
	// idle 池；未读即 Close 走 net/http earlyClose 路径弃连接（任意大小非空 body
	// 均触发，连接复用为零）。drain 在 result 构造（Latency 定格）之后的 defer 中
	// 执行，耗时不计入 Latency。io.Copy 返回值忽略：drain 失败仅意味着连接不复用
	// （Close 兜底弃连接），无错误语义可处理。
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBytes))
		resp.Body.Close()
	}()

	healthy := resp.StatusCode >= 200 && resp.StatusCode <= 399
	if c.expectedStatus > 0 {
		healthy = resp.StatusCode == c.expectedStatus
	}
	result := &checker.CheckResult{
		Healthy:    healthy,
		StatusCode: resp.StatusCode,
		Latency:    time.Since(start),
	}
	if !healthy {
		result.Err = fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	return result, nil
}

// newHTTPChecker 是 "http" 协议的 CheckerFactory。
// 解析 Extra["path"]（默认 "/health"）、Extra["method"]（默认 "GET"）、
// Extra["expected_status"]（默认空 = 区间判定 200-399；显式指定时合法口径
// [100,599]）；expected_status 非法（非数字 token 或越界数值，含显式 "0"/负值）
// 时返回 ErrInvalidExpectedStatus（不回显 Extra 值，docs/07 §5）。
func newHTTPChecker(cfg checker.TargetConfig) (checker.Checker, error) {
	if cfg.Addr == "" {
		return nil, checker.ErrEmptyTargetAddress // 共享 sentinel 直返（消息自包含 "checker: " 前缀，docs/06 §5）
	}
	c := &HTTPChecker{
		addr:    cfg.Addr,
		path:    "/health",
		method:  "GET",
		timeout: cfg.Timeout,
	}
	if v := cfg.Extra["path"]; v != "" {
		c.path = v
	}
	if v := cfg.Extra["method"]; v != "" {
		c.method = v
	}
	if v := cfg.Extra["expected_status"]; v != "" {
		n, err := strconv.Atoi(v)
		// 数值范围校验（2026-10-29 审计 P2-2 修复）：合法口径 [100,599]（HTTP 状态码全集）。
		// 显式 "0"/负值与 "0=未设置" 哨兵冲突（静默回退区间判定=配置被吞），
		// 越界正值恒不匹配（永久失败无诊断线索）——均 fail-fast 拒绝，
		// 不静默钳制或回退默认值（docs/05 §5 校验策略）。
		// sentinel 直返，不 wrap strconv 错误：其消息（parsing "..."）含 Extra 原值，
		// wrap 会违反 docs/07 §5（Extra 值不进入 error 消息），故丢弃 err。
		if err != nil || n < 100 || n > 599 {
			return nil, ErrInvalidExpectedStatus
		}
		c.expectedStatus = n
	}
	return c, nil
}

func init() {
	// blank import 触发注册（对齐 database/sql 驱动模式）。
	// 协议名取 checker.ProtocolHTTP（注册名单一事实源，固定非空）、factory 非 nil，
	// Register 不会失败（零日志，丢弃返回值）。
	_ = checker.Register(checker.ProtocolHTTP, newHTTPChecker)
}
