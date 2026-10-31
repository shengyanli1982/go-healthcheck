package healthcheck

import (
	"context"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shengyanli1982/go-healthcheck/passive"
)

// stubClock 是 ST-06 本地 Clock 测试替身。
// 命名刻意避开 fakeClock/fakeChecker/fakeScheduler/fakePassiveDetector——
// 那四个符号由 ST-07 在 healthcheck_basic_test.go 定义（同包，ST-08 复用不得重复定义），
// 此处使用 stub 前缀防止 Wave 3 合并时重复声明。
type stubClock struct {
	now time.Time
}

func (c *stubClock) Now() time.Time { return c.now }

// stubChecker 是 ST-06 本地 Checker 测试替身（命名理由同 stubClock）。
type stubChecker struct{}

func (stubChecker) Check(ctx context.Context) (*CheckResult, error) {
	return &CheckResult{Healthy: true}, nil
}

// TestNewDefaultConfig 对照 docs/05 §2/§4 默认值全表逐字段断言。
func TestNewDefaultConfig(t *testing.T) {
	cfg := newDefaultConfig()
	require.NotNil(t, cfg)

	assert.Equal(t, 5*time.Second, cfg.interval)
	assert.Equal(t, 2*time.Second, cfg.timeout)
	assert.Equal(t, 2, cfg.rise)
	assert.Equal(t, 3, cfg.fall)
	assert.Equal(t, runtime.NumCPU(), cfg.workers)
	assert.Equal(t, realClock{}, cfg.clock) // clock != nil 且为 realClock（不导出默认实现）
	assert.Equal(t, 16, cfg.spreadSlots)
	assert.Nil(t, cfg.onChange)
	assert.Nil(t, cfg.passiveDetector)
	assert.False(t, cfg.passiveDetectorSet) // 未显式设置 → ST-07 New() 构造默认 TimeWindowDetector
	assert.Equal(t, 3, cfg.passiveMaxFails)
	assert.Equal(t, 10*time.Second, cfg.passiveFailTimeout)
	assert.Nil(t, cfg.onInternalError)

	// 零配置即可生产使用（docs/05 §1）：默认配置必须直接通过校验
	assert.NoError(t, cfg.validate())
}

// TestOptionsApply 断言 12 个全局 Option（docs/05 §2，恰 12 个）逐个应用后
// managerConfig 对应字段 == 设定值。
func TestOptionsApply(t *testing.T) {
	cfg := newDefaultConfig()
	require.NotNil(t, cfg)

	clk := &stubClock{now: time.Unix(1700000000, 0)}
	det := passive.NewTimeWindowDetector(1, time.Second, realClock{})
	onChange := StateChangeCallback(func(host string, healthy bool, result *CheckResult) {})
	onErr := InternalErrorHandler(func(host string, err error) {})

	opts := []Option{
		WithInterval(7 * time.Second),
		WithTimeout(3 * time.Second),
		WithRise(5),
		WithFall(6),
		WithWorkers(9),
		WithClock(clk),
		WithSpreadSlots(8),
		WithOnStateChange(onChange),
		WithPassiveDetector(det),
		WithPassiveMaxFails(4),
		WithPassiveFailTimeout(15 * time.Second),
		WithOnInternalError(onErr),
	}
	require.Len(t, opts, 12) // API 面积契约：恰好 12 个全局 Option
	for _, opt := range opts {
		opt(cfg)
	}

	assert.Equal(t, 7*time.Second, cfg.interval)
	assert.Equal(t, 3*time.Second, cfg.timeout)
	assert.Equal(t, 5, cfg.rise)
	assert.Equal(t, 6, cfg.fall)
	assert.Equal(t, 9, cfg.workers)
	assert.Equal(t, clk, cfg.clock)
	assert.Equal(t, 8, cfg.spreadSlots)
	assert.Equal(t, det, cfg.passiveDetector)
	assert.True(t, cfg.passiveDetectorSet) // WithPassiveDetector 已显式调用
	assert.Equal(t, 4, cfg.passiveMaxFails)
	assert.Equal(t, 15*time.Second, cfg.passiveFailTimeout)
	// 函数值不可比较（reflect.DeepEqual 对非 nil func 恒 false）→ 以代码指针比对
	assert.Equal(t, reflect.ValueOf(onChange).Pointer(), reflect.ValueOf(cfg.onChange).Pointer())
	assert.Equal(t, reflect.ValueOf(onErr).Pointer(), reflect.ValueOf(cfg.onInternalError).Pointer())

	// 12 项组合后仍为合法配置（7s/8 ≈ 875ms ≥ 1ms）
	assert.NoError(t, cfg.validate())
}

// TestHostOptionsApply 断言 4 个 HostOption（docs/05 §3，恰 4 个）逐个应用后
// hostConfig 对应字段 == 设定值。
func TestHostOptionsApply(t *testing.T) {
	hc := &hostConfig{}
	chk := stubChecker{}

	opts := []HostOption{
		WithChecker(chk),
		WithProtocol("http"),
		WithHostInterval(9 * time.Second),
		WithHostTimeout(4 * time.Second),
	}
	require.Len(t, opts, 4) // API 面积契约：恰好 4 个 HostOption
	for _, opt := range opts {
		opt(hc)
	}

	assert.Equal(t, chk, hc.checker)
	assert.Equal(t, "http", hc.protocol)
	assert.Equal(t, 9*time.Second, hc.interval)
	assert.Equal(t, 4*time.Second, hc.timeout)
	assert.NoError(t, hc.validate("10.0.0.1:8080"))
}

// TestHostConfig_Validate 是 hostConfig.validate(host) 矩阵（docs/05 §5 host 级范围，
// ST-07 AddHost 调用）：零值 = 未覆盖哨兵合法；显式设置时 (0, 24h]；
// 负值与恰超上限非法。非法行携带三重契约断言：
// errSubstr 钉住哨兵错误的固定消息子串，
// "(host %q)" 后缀钉住全库统一上下文模板（docs/06 §5），
// wantErrIs 钉住复用全局哨兵的 errors.Is 同一性。
func TestHostConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		opts      []HostOption
		errSubstr string // "" = 期望 nil
		wantErrIs error  // 哨兵身份契约（合法行为 nil）
	}{
		{name: "zero_value_no_override", opts: nil},
		{name: "interval_at_max_24h", opts: []HostOption{WithHostInterval(24 * time.Hour)}},
		{name: "interval_over_max", opts: []HostOption{WithHostInterval(24*time.Hour + time.Millisecond)}, errSubstr: "interval", wantErrIs: ErrInvalidInterval},
		{name: "interval_negative", opts: []HostOption{WithHostInterval(-time.Second)}, errSubstr: "interval", wantErrIs: ErrInvalidInterval},
		{name: "timeout_at_max_24h", opts: []HostOption{WithHostTimeout(24 * time.Hour)}},
		{name: "timeout_over_max", opts: []HostOption{WithHostTimeout(24*time.Hour + time.Millisecond)}, errSubstr: "timeout", wantErrIs: ErrInvalidTimeout},
		{name: "timeout_negative", opts: []HostOption{WithHostTimeout(-time.Second)}, errSubstr: "timeout", wantErrIs: ErrInvalidTimeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hc := &hostConfig{}
			for _, opt := range tt.opts {
				opt(hc)
			}
			err := hc.validate("10.0.0.1:8080")
			if tt.errSubstr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.errSubstr)
			assert.ErrorContains(t, err, `(host "10.0.0.1:8080")`) // host 名上下文增量（统一后缀模板）
			assert.ErrorIs(t, err, tt.wantErrIs)
		})
	}
}

// TestRealClock 冒烟：realClock{}.Now() 与 time.Now() 差 <1s。
func TestRealClock(t *testing.T) {
	c := realClock{}
	assert.WithinDuration(t, time.Now(), c.Now(), time.Second)
}
