package passive

import (
	"sync"
	"time"
)

// hostWindow 是单个 host 的被动检测窗口状态（TimeWindowDetector 独占，SF4）。
type hostWindow struct {
	passiveHealthy bool      // 初始 true
	count          int       // 当前窗口内累计失败次数
	windowStart    time.Time // 当前窗口起点，初始零值（任意 now.Sub(零值) 饱和为最大 Duration → 首个失败必开新窗口）
}

// TimeWindowDetector 是 PassiveDetector 的内置实现，
// 对齐 nginx max_fails/fail_timeout 语义（docs/03 §1.4）：
//
//	failTimeout 窗口内累计 maxFails 次失败 → 标记被动不健康（翻转）；
//	窗口到期后由下一次 Record(host, true) 确定性恢复（惰性检查，无后台 goroutine）；
//	失败路径对已判死 host 只重开计数窗口，永不恢复。
//
// 内部互斥锁为 leaf lock（docs/03 §3.2）：持有期间不做回调/投递、
// 不获取任何其他锁；调用方亦不得在持有其他锁时调用本类型方法。
// 零值不可用，必须经 NewTimeWindowDetector 构造。
type TimeWindowDetector struct {
	maxFails    int
	failTimeout time.Duration
	clock       Clock
	mu          sync.Mutex // leaf lock：取后立即释放，永不嵌套其他锁（docs/03 §3.2）
	hosts       map[string]*hostWindow
}

// 编译期断言：TimeWindowDetector 满足 PassiveDetector 接口。
var _ PassiveDetector = (*TimeWindowDetector)(nil)

// NewTimeWindowDetector 创建 TimeWindowDetector。
// maxFails 为窗口内触发翻转的失败次数阈值（nginx max_fails），
// failTimeout 为计数窗口长度（nginx fail_timeout），
// clock 为注入的时间来源（由调用方保证非 nil，库自身不做防御性检查）。
func NewTimeWindowDetector(maxFails int, failTimeout time.Duration, clock Clock) *TimeWindowDetector {
	return &TimeWindowDetector{
		maxFails:    maxFails,
		failTimeout: failTimeout,
		clock:       clock,
		hosts:       make(map[string]*hostWindow),
	}
}

// Record 记录一次请求结果并返回 (flipped, newHealthy)。
//
// 语义钉死（docs/03 §1.4）：
//   - 失败路径：窗口过期（>= failTimeout，含零值 windowStart 的天然过期）则
//     重开窗口 count=1，否则 count++；count 达 maxFails 且当前健康 → 翻转为不健康。
//     失败路径永不恢复已不健康的 host。
//   - 成功路径：count 清零；仅当当前不健康且窗口已过期（>= failTimeout）时
//     恢复为健康并清空窗口（惰性恢复，本调用即触发点）。
//
// host 首次出现时惰性创建状态，被动健康初值 true。
// 整个判定在内部 leaf lock 临界区内完成，可安全并发调用。
func (d *TimeWindowDetector) Record(host string, success bool) (flipped bool, newHealthy bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	w := d.hosts[host]
	if w == nil {
		w = &hostWindow{passiveHealthy: true}
		d.hosts[host] = w
	}

	now := d.clock.Now()
	if !success {
		if now.Sub(w.windowStart) >= d.failTimeout { // 零值 windowStart → Sub 饱和为最大 Duration → 条件天然成立
			w.count = 1
			w.windowStart = now
		} else {
			w.count++
		}
		if w.count >= d.maxFails && w.passiveHealthy {
			w.passiveHealthy = false
			return true, false
		}
		return false, w.passiveHealthy
	}
	// success 路径
	w.count = 0
	if !w.passiveHealthy && now.Sub(w.windowStart) >= d.failTimeout {
		w.passiveHealthy = true
		w.windowStart = time.Time{}
		return true, true
	}
	return false, w.passiveHealthy
}

// Reset 重置指定 host 的被动检测状态（host 下线/重新注册时调用）。
// 清除后该 host 的下次 Record 将按初值（被动健康 true、无窗口）重新开始。
func (d *TimeWindowDetector) Reset(host string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.hosts, host)
}

// CurrentHealthy 返回指定 host 当前 detector 判定的被动健康值
// （可选能力接口 CurrentHealthyReader 的实现，r4 审计 P1-1 修复引入）。
// 与 Record/Reset 在同一 leaf lock 临界区内读取，调用方取到的值与最近一次
// Record 落地值同源同锁——这是根包 report 投递前复核的语义基础：
// 消灭「晚判定早投递」的 stale 快照覆盖，使投递流静止后镜像终态收敛到
// detector 当前值。
// 条目不存在时返回 (true, false)：true 为惰性建窗的初值语义（与 Record 的
// 惰性创建起步一致），ok=false 提示调用方该值非实测判定、应保持自身原值。
// O(1)、零分配。
func (d *TimeWindowDetector) CurrentHealthy(host string) (healthy bool, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	w := d.hosts[host]
	if w == nil {
		return true, false
	}
	return w.passiveHealthy, true
}
