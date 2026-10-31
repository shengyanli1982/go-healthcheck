package checker_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/shengyanli1982/go-healthcheck/checker"
	_ "github.com/shengyanli1982/go-healthcheck/checker/http" // 空白导入以注册 "http"（与使用方的接入方式一致）
	_ "github.com/shengyanli1982/go-healthcheck/checker/tcp"  // 空白导入以注册 "tcp"（与使用方的接入方式一致）
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// markerChecker 是带标记的自定义 Checker，用于区分不同 factory 的产物。
type markerChecker struct {
	tag string
}

func (m markerChecker) Check(ctx context.Context) (*checker.CheckResult, error) {
	return &checker.CheckResult{Healthy: true}, nil
}

func TestRegister_EmptyProtocol_Error(t *testing.T) {
	err := checker.Register("", func(cfg checker.TargetConfig) (checker.Checker, error) { return markerChecker{}, nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty protocol")
	assert.ErrorIs(t, err, checker.ErrEmptyProtocol) // sentinel 标识契约（docs/06 §5）
}

func TestRegister_NilFactory_Error(t *testing.T) {
	err := checker.Register("test-nil-factory", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil factory")
	assert.ErrorIs(t, err, checker.ErrNilFactory)
}

func TestRegister_DuplicateOverwrites(t *testing.T) {
	factoryA := func(cfg checker.TargetConfig) (checker.Checker, error) { return markerChecker{tag: "A"}, nil }
	factoryB := func(cfg checker.TargetConfig) (checker.Checker, error) { return markerChecker{tag: "B"}, nil }

	require.NoError(t, checker.Register("test-dup", factoryA))
	require.NoError(t, checker.Register("test-dup", factoryB))

	c, err := checker.NewChecker("test-dup", checker.TargetConfig{Addr: "127.0.0.1:1"})
	require.NoError(t, err)
	require.NotNil(t, c)

	mc, ok := c.(markerChecker)
	require.True(t, ok, "NewChecker product must come from the last registered factoryB (markerChecker type)")
	assert.Equal(t, "B", mc.tag)
}

func TestNewChecker_UnknownProtocol_Error(t *testing.T) {
	c, err := checker.NewChecker("unknown", checker.TargetConfig{Addr: "127.0.0.1:1"})
	require.Error(t, err, "unregistered protocol must return an error, not panic")
	assert.Nil(t, c)
	assert.Contains(t, err.Error(), "not registered")
	assert.ErrorIs(t, err, checker.ErrProtocolNotRegistered)
}

func TestProtocols_Sorted(t *testing.T) {
	// 稳健形态（任务卡注记）：断言切片已排序 + 包含 http 与 tcp 且二者相对顺序为字典序。
	// http/tcp 由本文件空白导入的 checker/http、checker/tcp 子包注册
	// （目录重组后内置协议与 grpc 同走空白导入注册路径，此处顺带验证该路径可用）。
	// 协议名字面量 "http"/"tcp" 有意保留（不改为 checker.Protocol* 常量）：
	// 子包 init() 已改用常量注册，此处以字面量取值钉住常量的线上值契约
	// （若常量值被误改，本测试必失败）。
	// 不用精确 DeepEqual，避免与同一测试二进制内其他测试注册的 test-* 协议产生文件顺序耦合；
	// 独立运行（-run TestProtocols_Sorted）时同样成立。
	p := checker.Protocols()
	assert.True(t, sort.StringsAreSorted(p), "Protocols() must be sorted lexicographically")

	httpIdx, tcpIdx := -1, -1
	for i, name := range p {
		if name == "http" {
			httpIdx = i
		}
		if name == "tcp" {
			tcpIdx = i
		}
	}
	require.NotEqual(t, -1, httpIdx, "builtin protocol http must be registered")
	require.NotEqual(t, -1, tcpIdx, "builtin protocol tcp must be registered")
	assert.Less(t, httpIdx, tcpIdx, "http must come before tcp (lexicographic)")
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	const (
		goroutines = 10
		iterations = 100
	)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			protocol := fmt.Sprintf("test-conc-%d", i)
			factory := func(cfg checker.TargetConfig) (checker.Checker, error) {
				return markerChecker{tag: protocol}, nil
			}
			for j := 0; j < iterations; j++ {
				// goroutine 内只用 assert，禁 require（docs/08 §1.1）。
				if !assert.NoError(t, checker.Register(protocol, factory)) {
					return
				}
				c, err := checker.NewChecker(protocol, checker.TargetConfig{Addr: "127.0.0.1:1"})
				if !assert.NoError(t, err) {
					return
				}
				assert.NotNil(t, c)
				_ = checker.Protocols()
			}
		}(i)
	}
	wg.Wait()
}
