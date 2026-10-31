package healthcheck

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestValidate_Matrix 覆盖 docs/05 §5 修订版校验全矩阵（DoD #8 配置侧）：
// 每参数至少覆盖"恰在上限内合法 / 恰超上限非法 / 零或负非法"，
// 外加毫秒精度组合约束（R-b：interval/spreadSlots ≥ 1ms，等值合法）
// 与显式 nil 注入 fail-fast（Want-Shift R2：WithClock(nil) / WithPassiveDetector(nil)）。
// 表驱动：mutate 在默认配置上修改单一维度；errSubstr 为空表示期望 validate() == nil。
// 双契约断言：errSubstr 钉住固定消息子串（定位非法项），wantErrIs 钉住哨兵身份
// （errors.Is 可判别——防 %v 包装导致静默失效，docs/06 §5）。
func TestValidate_Matrix(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*managerConfig)
		errSubstr string
		wantErrIs error // 哨兵身份契约（合法行为 nil）
	}{
		// —— 非法组 A：零/负（下限；每参数"零"与"负"各至少一例）——
		{name: "interval_zero", mutate: func(c *managerConfig) { c.interval = 0 }, errSubstr: "interval", wantErrIs: ErrInvalidInterval},
		{name: "interval_negative", mutate: func(c *managerConfig) { c.interval = -time.Millisecond }, errSubstr: "interval", wantErrIs: ErrInvalidInterval},
		{name: "timeout_zero", mutate: func(c *managerConfig) { c.timeout = 0 }, errSubstr: "timeout", wantErrIs: ErrInvalidTimeout},
		{name: "timeout_negative", mutate: func(c *managerConfig) { c.timeout = -time.Millisecond }, errSubstr: "timeout", wantErrIs: ErrInvalidTimeout},
		{name: "rise_zero", mutate: func(c *managerConfig) { c.rise = 0 }, errSubstr: "rise", wantErrIs: ErrInvalidRise},
		{name: "rise_negative", mutate: func(c *managerConfig) { c.rise = -1 }, errSubstr: "rise", wantErrIs: ErrInvalidRise},
		{name: "fall_zero", mutate: func(c *managerConfig) { c.fall = 0 }, errSubstr: "fall", wantErrIs: ErrInvalidFall},
		{name: "fall_negative", mutate: func(c *managerConfig) { c.fall = -1 }, errSubstr: "fall", wantErrIs: ErrInvalidFall},
		{name: "workers_zero", mutate: func(c *managerConfig) { c.workers = 0 }, errSubstr: "workers", wantErrIs: ErrInvalidWorkers},
		{name: "workers_negative", mutate: func(c *managerConfig) { c.workers = -1 }, errSubstr: "workers", wantErrIs: ErrInvalidWorkers},
		{name: "spreadSlots_zero", mutate: func(c *managerConfig) { c.spreadSlots = 0 }, errSubstr: "spreadSlots", wantErrIs: ErrInvalidSpreadSlots},
		{name: "spreadSlots_negative", mutate: func(c *managerConfig) { c.spreadSlots = -1 }, errSubstr: "spreadSlots", wantErrIs: ErrInvalidSpreadSlots},
		{name: "passiveMaxFails_zero", mutate: func(c *managerConfig) { c.passiveMaxFails = 0 }, errSubstr: "passiveMaxFails", wantErrIs: ErrInvalidPassiveMaxFails},
		{name: "passiveMaxFails_negative", mutate: func(c *managerConfig) { c.passiveMaxFails = -1 }, errSubstr: "passiveMaxFails", wantErrIs: ErrInvalidPassiveMaxFails},
		{name: "passiveFailTimeout_zero", mutate: func(c *managerConfig) { c.passiveFailTimeout = 0 }, errSubstr: "passiveFailTimeout", wantErrIs: ErrInvalidPassiveFailTimeout},
		{name: "passiveFailTimeout_negative", mutate: func(c *managerConfig) { c.passiveFailTimeout = -time.Millisecond }, errSubstr: "passiveFailTimeout", wantErrIs: ErrInvalidPassiveFailTimeout},

		// —— 非法组 B：恰超上限（一步非法，Want-Shift R2）——
		{name: "interval_over_max", mutate: func(c *managerConfig) { c.interval = 24*time.Hour + time.Millisecond }, errSubstr: "interval", wantErrIs: ErrInvalidInterval},
		{name: "timeout_over_max", mutate: func(c *managerConfig) { c.timeout = 24*time.Hour + time.Millisecond }, errSubstr: "timeout", wantErrIs: ErrInvalidTimeout},
		{name: "rise_over_max", mutate: func(c *managerConfig) { c.rise = 1001 }, errSubstr: "rise", wantErrIs: ErrInvalidRise},
		{name: "fall_over_max", mutate: func(c *managerConfig) { c.fall = 1001 }, errSubstr: "fall", wantErrIs: ErrInvalidFall},
		{name: "passiveMaxFails_over_max", mutate: func(c *managerConfig) { c.passiveMaxFails = 1001 }, errSubstr: "passiveMaxFails", wantErrIs: ErrInvalidPassiveMaxFails},
		{name: "workers_over_max", mutate: func(c *managerConfig) { c.workers = 4097 }, errSubstr: "workers", wantErrIs: ErrInvalidWorkers},
		{name: "spreadSlots_over_max", mutate: func(c *managerConfig) { c.spreadSlots = 4097 }, errSubstr: "spreadSlots", wantErrIs: ErrInvalidSpreadSlots},
		{name: "passiveFailTimeout_over_max", mutate: func(c *managerConfig) { c.passiveFailTimeout = 24*time.Hour + time.Millisecond }, errSubstr: "passiveFailTimeout", wantErrIs: ErrInvalidPassiveFailTimeout},

		// —— 非法组 C：毫秒精度组合约束（spreadSlots 既有约束维持，docs/05 §5）——
		// interval=4095ms + spreadSlots=4096 → interval/n < 1ms → 非法
		{
			name: "spreadSlots_interval_div_below_1ms",
			mutate: func(c *managerConfig) {
				c.interval = 4095 * time.Millisecond
				c.spreadSlots = 4096
			},
			errSubstr: "spreadSlots",
			wantErrIs: ErrInvalidSpreadPrecision,
		},

		// —— 非法组 D：显式 nil 注入（fail-fast，Want-Shift R2）——
		{name: "clock_explicit_nil", mutate: func(c *managerConfig) { WithClock(nil)(c) }, errSubstr: "clock", wantErrIs: ErrNilClock},
		{name: "passiveDetector_explicit_nil", mutate: func(c *managerConfig) { WithPassiveDetector(nil)(c) }, errSubstr: "passiveDetector", wantErrIs: ErrNilPassiveDetector},

		// —— 合法组（validate()==nil，边界"恰合法"全覆盖）——
		{name: "legal_all_defaults", mutate: func(c *managerConfig) {}},
		{name: "legal_interval_at_max_24h", mutate: func(c *managerConfig) { c.interval = 24 * time.Hour }},
		{name: "legal_timeout_at_max_24h", mutate: func(c *managerConfig) { c.timeout = 24 * time.Hour }},
		{name: "legal_passiveFailTimeout_at_max_24h", mutate: func(c *managerConfig) { c.passiveFailTimeout = 24 * time.Hour }},
		{name: "legal_rise_min_1", mutate: func(c *managerConfig) { c.rise = 1 }},
		{name: "legal_rise_max_1000", mutate: func(c *managerConfig) { c.rise = 1000 }},
		{name: "legal_fall_min_1", mutate: func(c *managerConfig) { c.fall = 1 }},
		{name: "legal_fall_max_1000", mutate: func(c *managerConfig) { c.fall = 1000 }},
		{name: "legal_passiveMaxFails_min_1", mutate: func(c *managerConfig) { c.passiveMaxFails = 1 }},
		{name: "legal_passiveMaxFails_max_1000", mutate: func(c *managerConfig) { c.passiveMaxFails = 1000 }},
		{name: "legal_workers_min_1", mutate: func(c *managerConfig) { c.workers = 1 }},
		{name: "legal_workers_max_4096", mutate: func(c *managerConfig) { c.workers = 4096 }},
		{name: "legal_spreadSlots_min_1", mutate: func(c *managerConfig) { c.spreadSlots = 1 }},
		// 默认 interval=5s：5s/4096≈1.22ms ≥ 1ms，恰在上限内合法
		{name: "legal_spreadSlots_max_4096", mutate: func(c *managerConfig) { c.spreadSlots = 4096 }},
		// interval/n 恰好 ==1ms，等值合法
		{
			name: "legal_spreadSlots_interval_exact_1ms",
			mutate: func(c *managerConfig) {
				c.interval = 4096 * time.Millisecond
				c.spreadSlots = 4096
			},
		},
		// nil = 默认禁用（与默认值一致），合法
		{name: "legal_onStateChange_nil", mutate: func(c *managerConfig) { WithOnStateChange(nil)(c) }},
		{name: "legal_onInternalError_nil", mutate: func(c *managerConfig) { WithOnInternalError(nil)(c) }},
		// timeout>interval 组合合法（建议非强制，docs/05 §5 原文不变）
		{
			name: "legal_timeout_greater_than_interval",
			mutate: func(c *managerConfig) {
				c.interval = time.Second
				c.timeout = 2 * time.Second
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newDefaultConfig()
			tt.mutate(cfg)
			err := cfg.validate()
			if tt.errSubstr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.errSubstr) // 固定消息子串契约
			assert.ErrorIs(t, err, tt.wantErrIs)       // 哨兵身份契约（%w 包装）
		})
	}
}
