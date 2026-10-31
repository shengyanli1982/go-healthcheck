package checker_test

// 基准测试套件（checker registry）——设计 SSOT：.agent-work-b652f803/specs/2026-10-29-benchmark-design.md §6.5。
// 覆盖热路径 6：registry RLock 查找 + factory 构造（NewChecker）、未注册协议
// error 路径（AddHost 失败路径对照项）、Protocols 快照 API。
//
// 包归属说明：与 registry_test.go 同用外部测试包 checker_test——内部测试包
// blank import checker/http（其反向 import checker）会构成 import cycle，
// 外部测试包是既有模式（设计 §4「同 registry_test.go 既有模式」）。
// registry 是全局 map，但基准只读（NewChecker/Protocols 走 RLock），不 Register
// → 无共享可变全局写入（M8）。
//
// 不测边界（§1.2）：Register 写路径（init 期一次性）、New()/validate 构造成本。

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/shengyanli1982/go-healthcheck/checker"
	_ "github.com/shengyanli1982/go-healthcheck/checker/http" // 空白导入以注册 "http"（与使用方的接入方式一致）
	_ "github.com/shengyanli1982/go-healthcheck/checker/tcp"  // 空白导入以注册 "tcp"（Protocols 注册数 ≥2）
	"github.com/stretchr/testify/require"
)

// BenchmarkNewChecker 测热路径 6：NewChecker 的 registry RLock 查找 + factory 构造。
// 口径：单线程；循环内不验返回值防开销（setup 期 require 验一次）；
// 循环外单次核对末轮返回值（防调用消失）。allocs/op 基线记录
// （http factory 构造 1 次堆分配预期；unknown 路径 = fmt.Errorf %w 包装分配）。
func BenchmarkNewChecker(b *testing.B) {
	cfg := checker.TargetConfig{Addr: "127.0.0.1:8080", Timeout: time.Second}

	b.Run("http", func(b *testing.B) {
		// setup 期 sanity 一次：http 协议已由 blank import 注册，factory 构造成功
		c, err := checker.NewChecker(checker.ProtocolHTTP, cfg)
		require.NoError(b, err, "http protocol must be registered via blank import")
		require.NotNil(b, c, "factory must return a Checker")

		var last checker.Checker
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			last, _ = checker.NewChecker(checker.ProtocolHTTP, cfg)
		}
		b.StopTimer()
		require.NotNil(b, last, "factory must keep returning a Checker across iterations")
	})

	b.Run("unknown-protocol", func(b *testing.B) {
		// setup 期 sanity 一次：未注册协议 → ErrProtocolNotRegistered（%w 包装可判别）
		_, err := checker.NewChecker("bench-unknown", cfg)
		require.Error(b, err, "unknown protocol must return error")
		require.True(b, errors.Is(err, checker.ErrProtocolNotRegistered),
			"error must be identifiable via errors.Is(ErrProtocolNotRegistered)")

		var lastErr error
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, lastErr = checker.NewChecker("bench-unknown", cfg)
		}
		b.StopTimer()
		require.Error(b, lastErr, "unknown protocol must keep returning error (B/op = wrap allocation baseline)")
	})
}

// BenchmarkProtocols 测快照 API：RLock + 切片构造 + sort（确定性字典序输出）。
// 口径：单线程；B/op 基线记录（每轮新建切片，不设零断言）。
func BenchmarkProtocols(b *testing.B) {
	// setup 期 sanity 一次：注册数 ≥2（http/tcp blank import）且字典序
	ps := checker.Protocols()
	require.NotEmpty(b, ps, "protocols must be registered via blank imports")
	require.True(b, sort.StringsAreSorted(ps), "protocols must be sorted lexicographically")

	var last []string
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		last = checker.Protocols()
	}
	b.StopTimer()
	require.NotEmpty(b, last, "snapshot must stay non-empty across iterations")
	require.True(b, sort.StringsAreSorted(last), "snapshot must stay sorted (deterministic output)")
}
