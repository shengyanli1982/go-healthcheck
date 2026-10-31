#!/usr/bin/env bash
# scripts/audit.sh — docs/11 §5 DoD 13 门禁本地审计脚本（G6 裁定：v1 不引入 CI 平台配置）。
# 用法：bash scripts/audit.sh（仓库根执行；任何门禁 FAIL → 退出码非 0）。
# 人工评审项（DoD #6、#1/#13 的文档部分、SF 追溯）在 dod-audit.md 中记录，本脚本只固化可程序化断言。
#
# 与 runbook ST-09 Step 3 模板的三处已裁定/实证修正（详见 dod-audit.md 偏差登记）：
# (1) DoD#9 命令1 `go mod graph | grep -icE 'log|slog'`==0 为 runbook 命令缺陷：grpc v1.75.1 自身
#    go.mod 元数据含 glog/go-logr/stdr 边（grpc 的测试依赖，恒 3 命中，且不在任何构建闭包内）。
#    替换为三条更强断言（主模块直接边 + 全闭包第三方日志模块 + 直接 Imports），意图不变。
# (2) 全局复核新增 P0 禁用项 grep，pattern 精确化为 `InFlightQueue|NewTimerQueueWithConfig`
#    （W1 审查 P2-2：宽 pattern 'InFlight' 会误命中契约测试名 TestTimerQueue_DrainWaitsInFlight）。
# (3) 全局复核新增 API 面积审计 `go doc . | grep -c 'func With'`==16，非锚定模式
#    （W2 审查 INFO-2：'^func With' 行首锚定与 go doc 分组缩进形态不匹配，实测 0≠16）。
set -u
cd "$(dirname "$0")/.." || exit 1
fail=0
pass() { printf 'DoD#%-2s PASS  %s\n' "$1" "$2"; }
bad()  { printf 'DoD#%-2s FAIL  %s\n' "$1" "$2"; fail=1; }
expect() { # expect <dod#> <desc> <actual> <want>
  if [ "$3" = "$4" ]; then pass "$1" "$2"; else bad "$1" "$2 (got: $3, want: $4)"; fi
}

# DoD #1  M1：四行接入示例测试可运行
if go test -run Example -count=1 . >/dev/null 2>&1; then pass 1 "M1 example 可运行"; else bad 1 "M1 example"; fi
# DoD #2  M2 全绿（C1-C10 映射表核对为人工项，见 dod-audit.md）
if go test -count=1 ./... >/dev/null 2>&1; then pass 2 "go test ./... 全绿"; else bad 2 "go test ./..."; fi
# DoD #3  M4：state/passive/scheduler 零协议特化
n=$(grep -rn 'http\|tcp\|grpc' state/ passive/ scheduler/ --include='*.go' 2>/dev/null | grep -v _test | wc -l | tr -d ' ')
expect 3 "核心三包零协议特化" "$n" "0"
# DoD #4  -race -count=100 全绿（重门禁；耗时数分钟属正常）
if go test -race -count=100 ./... >/dev/null 2>&1; then pass 4 "-race -count=100 全绿"; else bad 4 "-race -count=100"; fi
# DoD #5  依赖审计（单模块口径：根包构建闭包无 grpc；模块级 require 因 checker/grpc 同模块存在，见 dod-audit.md 注记）
n=$(go list -f '{{.Deps}}' . | tr ' ' '\n' | grep -c 'google.golang.org/grpc')
expect 5 "根包 Deps 无 grpc" "$n" "0"
n=$(go list -f '{{.Imports}}' ./checker ./checker/http ./checker/tcp ./state ./passive ./scheduler | grep -c grpc)
expect 5 "核心包 Imports 无 grpc" "$n" "0"
# DoD #6  SF1-SF6 落点追溯（人工评审项）
pass 6 "SF1-SF6 追溯表见 dod-audit.md（人工评审项）"
# DoD #7  幂等 TimerQueue 自测三件套（C-5）
if go test -race -count=1 -run 'TestTimerQueue_' ./scheduler/ >/dev/null 2>&1; then pass 7 "C-5 三件套通过"; else bad 7 "C-5 三件套"; fi
# DoD #8  错误传播：docs/06 §1.1 表全行命名用例
if go test -count=1 -run 'TestNew_InvalidConfig|TestAddHost_Errors|TestRemoveHost_NotFound|TestStart_AlreadyStarted|TestRegister_|TestNewChecker_|TestValidate_Matrix' ./... >/dev/null 2>&1; then pass 8 "错误传播用例"; else bad 8 "错误传播用例"; fi
# DoD #9  零日志审计（修正(1)：原命令1为 runbook 缺陷，替换为三条精确断言 + 模板原样两条源码 grep）
n=$(go mod graph | grep '^github.com/shengyanli1982/go-healthcheck ' | grep -icE 'log|slog')
expect 9 "模块图主模块直接边无日志库" "$n" "0"
n=$(go list -f '{{range .Deps}}{{println .}}{{end}}' ./... | sort -u | grep -cE '^github.com/golang/glog|^github.com/go-logr|^github.com/sirupsen/logrus|^go.uber.org/zap|^github.com/rs/zerolog')
expect 9 "全包构建闭包无第三方日志模块" "$n" "0"
n=$(go list -f '{{join .Imports "\n"}}' ./... | grep -cxE 'log|log/slog')
expect 9 "全包直接 Imports 无 std log/slog" "$n" "0"
n=$(grep -rnE '"log"|"log/slog"' --include='*.go' . | wc -l | tr -d ' ')
expect 9 "源码无 log/slog import" "$n" "0"
n=$(grep -rnE 'log\.(Print|Fatal|Panic)|slog\.' --include='*.go' . | grep -v _test | wc -l | tr -d ' ')
expect 9 "库代码无日志调用" "$n" "0"
# DoD #10 回调并发门禁（oracle + 不变式；反证记录为人工项见 dod-audit.md）
if go test -race -count=1 -run 'TestCallbackOrdering_|TestInvariant_' . >/dev/null 2>&1; then pass 10 "oracle+不变式通过"; else bad 10 "oracle/不变式"; fi
# DoD #11 回调 panic 隔离
if go test -race -count=1 -run 'TestErrorPropagation_CallbackPanic|TestNotifyLocked_' ./... >/dev/null 2>&1; then pass 11 "panic 隔离用例"; else bad 11 "panic 隔离"; fi
# DoD #12 无 ErrorHandler 残留（docs 侧 + 源码侧）
n=$(grep -rnE '(^|[^A-Za-z])ErrorHandler\b|WithErrorHandler' docs/ --exclude=ADR-001.md --exclude=11-scope-and-dod.md | wc -l | tr -d ' ')
expect 12 "docs 侧 ErrorHandler 残留" "$n" "0"
n=$(grep -rnE '(^|[^A-Za-z])ErrorHandler\b|WithErrorHandler' --include='*.go' . | wc -l | tr -d ' ')
expect 12 "源码侧 ErrorHandler 残留" "$n" "0"
# DoD #13 InternalErrorHandler 唯一载荷文档化（锚点短语存在即可，避免 godoc 换行/复现次数导致脆弱断言）
n=$(go doc . InternalErrorHandler 2>/dev/null | grep -c '唯一载荷')
if [ "$n" -ge 1 ]; then pass 13 "godoc 唯一载荷声明"; else bad 13 "godoc 唯一载荷声明 (got: $n, want >=1)"; fi
# 全局复核：build/vet/recover 恰好 2 处/禁项 grep/P0 禁用项（修正(2)）/API 面积（修正(3)）
if go build ./... && go vet ./...; then pass 0 "build+vet"; else bad 0 "build+vet"; fi
n=$(grep -rn 'recover()' --include='*.go' . | grep -v _test | wc -l | tr -d ' ')
expect 0 "recover 恰好 2 处" "$n" "2"
n=$(grep -rn 'math/rand\|testing/synctest' --include='*.go' . | wc -l | tr -d ' ')
expect 0 "禁项(math/rand|synctest)为零" "$n" "0"
n=$(grep -rn 'InFlightQueue\|NewTimerQueueWithConfig' --include='*.go' . | wc -l | tr -d ' ')
expect 0 "P0 禁用项(InFlightQueue|NewTimerQueueWithConfig)为零" "$n" "0"
n=$(grep -c ') Option {' options.go)
expect 0 "API 面积: Option 恰 12" "$n" "12"
n=$(grep -c ') HostOption {' options.go)
expect 0 "API 面积: HostOption 恰 4" "$n" "4"
n=$(go doc . | grep -c 'func With')
expect 0 "API 面积: go doc With* 恰 16" "$n" "16"
# 非 ASCII 字符串/字符字面量审计（P3-A 防复发，2026-10-30 quality-review-r2）：AST 级扫描。
# go/parser 解析全部 .go（含测试），遍历 ast.BasicLit STRING/CHAR 的源码原文；
# COMMENT token 不进入 AST 字面量节点（中文注释合法，天然排除，grep 类断言无法区分故须 AST）。
# 扫描程序解析/遍历失败时以非零码退出 → 判 FAIL，防止空输出假 PASS。
# 变异验证记录：向任一 .go 字符串字面量插入非 ASCII 字符 → 本断言 FAIL（还原后复跑 PASS）。
nondir=$(mktemp -d)
cat > "$nondir/nonascii.go" <<'GOEOF'
// nonascii.go — AST 级非 ASCII 字符串/字符字面量扫描器（audit.sh 全局复核断言，P3-A 防复发）。
// 方法学：go/parser 解析 .go 文件，遍历 ast.BasicLit（STRING/CHAR）；COMMENT token 不进入
// AST 字面量节点，中文注释天然被排除（项目规范：非 ASCII 仅允许 COMMENT token）。
// 输出：每处违规字面量一行（file:line:col），由调用方计数并断言 == 0。
// 扫描自身失败（解析/遍历错误）时以非零码退出，防止空计数造成假 PASS。
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	fset := token.NewFileSet()
	scanErr := false
	walkErr := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != "." && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir // 隐藏目录（.git 等）非 Go 源码，跳过
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			fmt.Fprintf(os.Stderr, "parse %s: %v\n", path, perr)
			scanErr = true
			return nil
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || (lit.Kind != token.STRING && lit.Kind != token.CHAR) {
				return true
			}
			for _, r := range lit.Value { // lit.Value 为源码原文（含引号），非 ASCII 字符以原样出现
				if r > 127 {
					pos := fset.Position(lit.Pos())
					fmt.Printf("%s:%d:%d: non-ASCII in %s literal\n", pos.Filename, pos.Line, pos.Column, lit.Kind)
					break // 每处字面量只计一次
				}
			}
			return true
		})
		return nil
	})
	if walkErr != nil {
		fmt.Fprintln(os.Stderr, "walk:", walkErr)
		scanErr = true
	}
	if scanErr {
		os.Exit(2) // 扫描不完整：调用方报 FAIL，而非空计数假 PASS
	}
}
GOEOF
scanout=$(go run "$nondir/nonascii.go" 2>&1); scanrc=$?
rm -rf "$nondir"
if [ "$scanrc" -ne 0 ]; then
  bad 0 "非 ASCII 字面量审计(AST 级)扫描失败 (rc=$scanrc): $scanout"
else
  n=$(printf '%s' "$scanout" | grep -c '[^[:space:]]')
  expect 0 "字符串/字符字面量非 ASCII 为零(AST 级，排除 COMMENT)" "$n" "0"
fi

if [ "$fail" = "0" ]; then echo "== ALL GATES PASS =="; else echo "== GATES FAILED =="; fi
exit "$fail"
