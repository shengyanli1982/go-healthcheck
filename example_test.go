package healthcheck_test // 外部测试包：模拟消费者真实接入形态（根包 + blank import 协议子包，M1 四行接入）

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	healthcheck "github.com/shengyanli1982/go-healthcheck"
	"github.com/shengyanli1982/go-healthcheck/checker"        // 协议名常量 checker.ProtocolHTTP（内置协议注册名单一事实源）
	_ "github.com/shengyanli1982/go-healthcheck/checker/http" // 空白导入注册 "http" 协议（database/sql 驱动模式）
)

func Example() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()

	// —— M1 核心 4 行：New → AddHost → Start → GetHealthyHosts ——
	hc, _ := healthcheck.New(healthcheck.WithInterval(20 * time.Millisecond)) // 示例缩短间隔以便运行（须满足 interval/spreadSlots ≥ 1ms：默认 16 槽 → interval ≥ 16ms）；零配置 New() 同样可跑通
	_ = hc.AddHost(srv.Listener.Addr().String(), healthcheck.WithProtocol(checker.ProtocolHTTP))
	_ = hc.Start(context.Background())
	defer hc.Stop(context.Background())

	deadline := time.Now().Add(2 * time.Second) // 默认 rise=2：等待两轮探测
	for len(hc.GetHealthyHosts()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	fmt.Println(len(hc.GetHealthyHosts()))
	// Output: 1
}
