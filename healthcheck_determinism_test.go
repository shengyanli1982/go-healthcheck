package healthcheck

// ST-08 确定性测试：docs/08 §4 确定性断言规格（M2-C3）逐字落实。
// 复用 healthcheck_basic_test.go 的 fake 辅助（fakeClock/fakeChecker/
// fakeScheduler/newTestManager），不重复定义。
//
// 确定性承诺为事件序列级（SF5）：fakeClock+fakeScheduler seam 下无任何
// 真实时间与调度参与，同序列 100 次运行输出必须逐字节相同；
// 门禁：go test -race -count=100 无 data race、无结果漂移。
// 事件序列为固定序列（禁用随机数，G-3 审计纪律）。

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/shengyanli1982/go-healthcheck/checker"
)

// TestDeterminism_C3_100RunsByteIdentical（docs/08 §4 规格）：
// 创建 HealthManager（注入 fakeClock + fakeChecker）→ AddHost 3 个 host →
// 喂入固定 CheckResult 事件序列 S,S,F,S,F,F,S → 每步推进 fakeClock 固定量 →
// 记录 GetHealthyHosts()/IsHealthy() 输出 → 重复 100 次 → 100 次结果逐字节相同。
//
// 实现说明：任务卡伪代码"fcChecker.下一条结果 = ev"以预编程结果序列等价落实——
// probeFn 为同包直调（顺序确定），21 次 Check 调用的结果可预先按
// 事件×host 展开顺序固定（basic_test fakeChecker 契约：按 results 序列依次
// 返回），与"每次探测前设置下一条结果"语义一致且无共享可变状态。
func TestDeterminism_C3_100RunsByteIdentical(t *testing.T) {
	hosts := []string{"h1", "h2", "h3"}
	// 固定事件序列（docs/08 §4 示例：S,S,F,S,F,F,S）
	events := []bool{true, true, false, true, false, false, true}

	const runs = 100
	first := ""
	for run := 0; run < runs; run++ {
		// 预编程结果序列：按 (事件 × host) 展开顺序，第 k 次 Check 返回 events[k/3]。
		// fixture 每 run 独立构造（L5 P3-3）：safeCheck 对零值 Timestamp 原地补填（M-6），
		// 跨 run 共享 *CheckResult 指针会携带上轮补填的旧 Timestamp，
		// 与 R-d"每轮探测新建"字面冲突——每轮新建消除隐患（真实 Checker 每次新建，行为一致）
		results := make([]fakeCheckResult, 0, len(events)*len(hosts))
		for _, ev := range events {
			for range hosts {
				results = append(results, fakeCheckResult{result: &checker.CheckResult{Healthy: ev}})
			}
		}
		fc := newFakeClock()
		fs := &fakeScheduler{}                                              // seam 替换，不起真实调度（SF5：排除调度时刻不确定性）
		m := newTestManager(t, fs, WithClock(fc), WithRise(2), WithFall(3)) // rise/fall 固定
		fchk := &fakeChecker{results: results}
		for _, h := range hosts {
			require.NoError(t, m.AddHost(h, WithChecker(fchk)))
		}

		var record strings.Builder
		for _, ev := range events {
			for _, h := range hosts {
				fc.Advance(time.Second) // 每步推进 fakeClock 固定量
				m.probeFn(context.Background(), h)
				// 字符串累积：事件值 + GetHealthyHosts 快照 + IsHealthy
				fmt.Fprintf(&record, "%v|%v|%v\n", ev, m.GetHealthyHosts(), m.IsHealthy(h))
			}
		}
		got := record.String()

		if run == 0 {
			first = got
			// 健全性自检：记录必须反映状态机演进（非全空/全同），否则断言无意义
			assert.NotEmpty(t, first)
			assert.Contains(t, first, "true|[h1 h2 h3]|true", "all three hosts should be healthy at the sequence tail (rise=2 satisfied)")
			continue
		}
		assert.Equal(t, first, got, "run %d must be byte-identical to run 0 (C3 determinism)", run)
	}
}
