# go-healthcheck

[![Go Report Card](https://goreportcard.com/badge/github.com/shengyanli1982/go-healthcheck)](https://goreportcard.com/report/github.com/shengyanli1982/go-healthcheck)
[![Go Reference](https://pkg.go.dev/badge/github.com/shengyanli1982/go-healthcheck.svg)](https://pkg.go.dev/github.com/shengyanli1982/go-healthcheck)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/shengyanli1982/go-healthcheck)

Embeddable health-check library for Go gateways, proxies, and load balancers. It pairs active probing (HTTP / TCP / gRPC) with passive failure detection (nginx-style `max_fails` / `fail_timeout` time windows) and collapses both into structured probe facts and aggregated health signals. One design rule runs through the whole API: **the library outputs facts, not decisions** — routing, load balancing, and traffic policy belong to the host.

**Features:**

- **Active + passive convergence** — a rise/fall probe state machine (nginx semantics) ANDed with a `max_fails`/`fail_timeout` traffic window, so real request failures evict a host without waiting for the next probe round.
- **Deterministic by construction** — probe offsets derive from the registration index, not RNG (`math/rand` is banned codebase-wide); with an injected clock, the same event sequence reproduces the same states.
- **Three protocols, driver-style registration** — HTTP, TCP, and gRPC checkers live in blank-importable subpackages (the `database/sql` driver model); a custom protocol is one `Check` method away.
- **Zero log noise** — the library never imports `log` or `log/slog`; every failure surfaces through one of four explicit channels.
- **Hot-path friendly** — reads and passive reports are O(1) and allocation-free, locked in by zero-alloc guard tests.
- **Contained panics, fail-loud core** — panics from your checkers and callbacks are isolated and reported; the library never recovers its own bugs.

**Requires Go 1.23+.**

## Install

```bash
go get github.com/shengyanli1982/go-healthcheck
```

The core runtime depends on exactly one module: `github.com/shengyanli1982/workqueue/v2` (TimerQueue scheduling). `google.golang.org/grpc` enters the build only when you blank-import the `checker/grpc` subpackage; `testify` is test-only.

## Quick Start

Four lines do the work: **New → AddHost → Start → GetHealthyHosts**.

```go
package main

import (
	"context"
	"fmt"
	"time"

	healthcheck "github.com/shengyanli1982/go-healthcheck"
	"github.com/shengyanli1982/go-healthcheck/checker"        // protocol name constants
	_ "github.com/shengyanli1982/go-healthcheck/checker/http" // blank import: registers the "http" protocol
)

func main() {
	hc, err := healthcheck.New() // zero config: 5s interval, 2s timeout, rise=2, fall=3
	if err != nil {
		panic(err) // invalid config fails fast — never silently falls back
	}

	// Registration cannot fail in this demo; check the error in real code.
	_ = hc.AddHost("10.0.0.1:8080", healthcheck.WithProtocol(checker.ProtocolHTTP))
	_ = hc.AddHost("10.0.0.2:8080", healthcheck.WithProtocol(checker.ProtocolHTTP))

	if err := hc.Start(context.Background()); err != nil {
		panic(err)
	}
	defer func() { _ = hc.Stop(context.Background()) }() // graceful: stop now + bounded drain

	// rise=2: a host needs 2 consecutive probe successes to turn healthy,
	// i.e. roughly two 5s rounds after Start. Poll until both hosts are up.
	deadline := time.Now().Add(12 * time.Second)
	for len(hc.GetHealthyHosts()) < 2 && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
	}

	for _, host := range hc.GetHealthyHosts() { // sorted snapshot, always
		fmt.Println("healthy:", host)
	}
	// With both upstreams answering 2xx:
	// healthy: 10.0.0.1:8080
	// healthy: 10.0.0.2:8080
}
```

A runnable version lives in [example_test.go](example_test.go) (`go test -run Example .`).

## How It Works

### Health state convergence

```
            ACTIVE PATH                                     PASSIVE PATH
  ┌───────────────────────────────┐               ┌───────────────────────────────┐
  │  worker pool probes each host │               │  your traffic reports via     │
  │  over HTTP / TCP / gRPC       │               │  ReportSuccess/ReportFailure  │
  │  period = interval + probe    │               │  (real request outcomes)      │
  │              │                │               │              │                │
  │              ▼                │               │              ▼                │
  │  rise/fall state machine      │               │  TimeWindowDetector           │
  │  rise=2 → UP   fall=3 → DOWN  │               │  3 fails / 10s window → DOWN  │
  └──────────────┬────────────────┘               └──────────────┬────────────────┘
                 │                                               │
                 └─────────────────────┬─────────────────────────┘
                                       ▼
                          overall = active AND passive
                                       │
                                       ▼
               IsHealthy / GetHealthyHosts / StateChangeCallback
```

| Signal  | Driven by             | Flips at defaults                                                          |
| ------- | --------------------- | -------------------------------------------------------------------------- |
| Active  | worker-pool probes    | `rise=2` consecutive successes → up; `fall=3` consecutive failures → down  |
| Passive | your traffic reports  | `max_fails=3` failures inside `fail_timeout=10s` → down                   |
| Overall | —                     | `IsHealthy` = active AND passive; either side can veto                     |

A freshly added host starts unhealthy (safe cold start) and turns healthy only after `rise` consecutive probe successes. A host that is passively down recovers on the first success recorded after its window has elapsed (nginx semantics); any success resets the failure counter.

### Scheduling

- **Deterministic spread** — a host's first probe is offset by `interval × (index % spreadSlots) / spreadSlots`. Offsets come from the registration index, not from randomness: no thundering herd, and the schedule is reproducible.
- **Fixed delay** — a host's period is `interval + probe duration`; the next round is scheduled only after the current one completes, matching nginx/Envoy semantics. Slow hosts never accumulate backlog.
- **Single-writer probing** — the TimerQueue's idempotent Get–Done mode guarantees at most one worker probes a given host at any instant.
- **Event-sequence determinism** — the same `CheckResult` event sequence plus the same injected `Clock` yields identical states and query results; `WithClock` covers `CheckResult.Timestamp` and the passive time windows.

### Error channels

The library logs nothing — no `log`/`log/slog` import anywhere. Everything surfaces through four channels you control:

| Channel                    | Carries                                                        | Where you receive it                                            |
| -------------------------- | -------------------------------------------------------------- | ---------------------------------------------------------------- |
| Synchronous API error      | invalid config, lifecycle misuse, host-registration errors     | return values of `New`/`Start`/`Stop`/`AddHost`/`RemoveHost`     |
| `CheckResult.Err`          | why a probe failed — including contained checker panics        | `GetLastResult(host)`, state-change callback                     |
| State-change callback      | health transitions                                             | `WithOnStateChange`                                              |
| Internal-error handler     | panics recovered from your callback — its only payload         | `WithOnInternalError`                                            |

Exactly two `recover` points exist in the entire library, both at consumer boundaries (checker and callback). Library bugs propagate and crash loudly; they are never swallowed.

### Lifecycle

| Event                            | Behavior                                                                                                                                                                        |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Start(ctx)` twice               | second call returns an error                                                                                                                                                    |
| `Start` after `Stop`             | returns an error — the lifecycle is one-shot                                                                                                                                    |
| `Stop(ctx)`                      | stops scheduling immediately, then bounded-waits for in-flight probes; ctx timeout returns `ctx.Err()`. `IsHealthy`/`GetHealthyHosts`/`GetLastResult` keep answering afterwards |
| Parent ctx canceled (not `Stop`) | active probing stalls with zero signal — no errors, no probe output, `IsHealthy` freezes its last value; the passive path stays alive. Cancellation alone does not reclaim the TimerQueue goroutine started by `New`; pass a ctx matching the manager's lifetime and call `Stop` to finish |

## API Reference

### Manager

| Method                                          | Description                                                                                     |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `New(opts ...Option) (*HealthManager, error)`   | create a manager; zero config is production-ready; invalid config fails fast                    |
| `Start(ctx context.Context) error`              | start the worker pool (non-blocking); double-start and start-after-stop return errors           |
| `Stop(ctx context.Context) error`               | graceful stop; query APIs stay available afterwards                                             |
| `AddHost(host string, opts ...HostOption) error` | register a host — requires `WithProtocol` or `WithChecker`; empty/duplicate/stopped → error     |
| `RemoveHost(host string) error`                 | deregister a host: cancel its schedule, reset its passive state                                 |
| `IsHealthy(host string) bool`                   | overall health (active AND passive); false for unknown hosts                                    |
| `GetHealthyHosts() []string`                    | healthy hosts as a lexicographically sorted snapshot                                            |
| `GetLastResult(host string) *CheckResult`       | last probe result (immutable, safe to hold); nil if unknown or never probed                     |
| `Hosts() int`                                   | number of registered hosts                                                                      |
| `ReportSuccess(host string)`                    | passive success from real traffic; O(1), zero allocation                                        |
| `ReportFailure(host string)`                    | passive failure from real traffic; O(1), zero allocation                                        |

`GetHealthyHosts` is a control-plane snapshot API — on the per-request hot path, cache the healthy set from `WithOnStateChange` instead. The library applies no address filtering: you own the trust boundary for everything passed to `AddHost`. If addresses can originate from user input, validate them before calling `AddHost` — or inject a custom checker with address filtering.

### Global options

| Option                       | Default              | Valid range                     |
| ---------------------------- | -------------------- | ------------------------------- |
| `WithInterval(d)`            | 5s                   | (0, 24h]                        |
| `WithTimeout(d)`             | 2s                   | (0, 24h]                        |
| `WithRise(n)`                | 2                    | [1, 1000]                       |
| `WithFall(n)`                | 3                    | [1, 1000]                       |
| `WithWorkers(n)`             | `runtime.NumCPU()`   | [1, 4096]                       |
| `WithSpreadSlots(n)`         | 16                   | [1, 4096]¹                      |
| `WithPassiveMaxFails(n)`     | 3                    | [1, 1000]                       |
| `WithPassiveFailTimeout(d)`  | 10s                  | (0, 24h]                        |
| `WithClock(c)`               | system clock         | explicit nil → error            |
| `WithOnStateChange(cb)`      | nil (disabled)       | nil is legal                    |
| `WithOnInternalError(fn)`    | nil (drop panics)    | nil is legal                    |
| `WithPassiveDetector(d)`     | TimeWindowDetector   | explicit nil → error            |

¹ Also requires `interval ≥ slots × 1ms` (TimerQueue millisecond precision) — `New` rejects the combination otherwise.

Invalid values fail fast: `New()`/`AddHost()` return an error, never a silent fallback to defaults.

### Host options

| Option                  | Purpose                                                                                              |
| ----------------------- | ---------------------------------------------------------------------------------------------------- |
| `WithProtocol(name)`    | create a checker by protocol name via the Registry (requires the matching blank import)              |
| `WithChecker(c)`        | pass a checker instance directly; takes precedence when both are set — one of the two is required    |
| `WithHostInterval(d)`   | per-host interval override; zero = inherit the global value                                          |
| `WithHostTimeout(d)`    | per-host timeout override; zero = inherit the global value                                           |

`AddHost` always builds a bare `TargetConfig`. Protocol-specific settings (gRPC TLS/service, HTTP expected status) are reachable only by constructing the checker via `checker.NewChecker(..., checker.TargetConfig{Extra: ...})` and passing it with `WithChecker`.

### Protocols

| Protocol | Constant               | Blank import                                                  | Healthy means                          |
| -------- | ---------------------- | ------------------------------------------------------------- | -------------------------------------- |
| HTTP     | `checker.ProtocolHTTP` | `_ "github.com/shengyanli1982/go-healthcheck/checker/http"`   | GET status in [200, 399]               |
| TCP      | `checker.ProtocolTCP`  | `_ "github.com/shengyanli1982/go-healthcheck/checker/tcp"`    | TCP connect succeeds                   |
| gRPC     | `checker.ProtocolGRPC` | `_ "github.com/shengyanli1982/go-healthcheck/checker/grpc"`   | Health RPC returns SERVING             |

Registration follows the `database/sql` driver model: each subpackage registers itself in `init()`, so a missing blank import makes `AddHost` return a "protocol not registered" error. The constants are the single source of truth for built-in names; custom protocols may register any string via `checker.Register(name, factory)`.

The `Checker` return contract mirrors `net/http.RoundTripper`:

- `(*CheckResult, nil)` — the probe ran; the facts live in the result.
- `(nil, error)` — the probe itself could not execute; the library synthesizes `Healthy=false` and counts one failure.
- Panics inside a checker are contained and reported through `CheckResult.Err`; the worker survives.

One caveat on the facts: `CheckResult.Latency` is probe latency (connect/handshake level), not average request latency. Never feed it to least-time selectors.

## Usage Examples

### Passive detection on the request path

```go
// Wire passive detection into your forwarding path.
// ReportSuccess / ReportFailure are O(1) and allocation-free: hot-path safe.
func serveUpstream(hc *healthcheck.HealthManager, upstream string) {
	if err := forward(upstream); err != nil { // your forwarding logic
		// 3 failures within the 10s window (max_fails=3, fail_timeout=10s)
		// flip the passive side down — without waiting for the next active probe.
		hc.ReportFailure(upstream)
		return
	}
	// A success resets the failure counter; a host that is passively down
	// recovers on the first success recorded after its window has elapsed.
	hc.ReportSuccess(upstream)
}
```

Overall health is the AND of both paths: a host healthy on the active path still reports unhealthy while its passive window says down, so real traffic failures evict a host immediately.

### State change callback

```go
hc, err := healthcheck.New(
	healthcheck.WithOnStateChange(func(host string, healthy bool, result *healthcheck.CheckResult) {
		// Called on every health flip:
		//   same host   → serialized, in event order
		//   other hosts → concurrent, no ordering guarantee
		//   result      → nil on passive flips
		//
		// Return fast. Calling manager APIs that take internal locks
		// (Start/Stop/AddHost/RemoveHost/Report*) from inside this callback
		// deadlocks — see the Concurrency section for the
		// forbidden/whitelist table. A panic here is contained and routed
		// to WithOnInternalError.
		fmt.Printf("%s healthy=%v\n", host, healthy)
	}),
)
```

Use the callback to maintain your own healthy-set cache — that is the intended hot-path pattern, since `GetHealthyHosts` is a control-plane snapshot.

### Custom checker

```go
type dbChecker struct{ dsn string }

// One method is the whole contract. Implementations must be concurrency-safe
// (one instance may probe several hosts) and must respect ctx.
// Return contract: (result, nil) = probe ran; (nil, error) = probe could not
// run — the library synthesizes Healthy=false and counts one failure.
func (c *dbChecker) Check(ctx context.Context) (*healthcheck.CheckResult, error) {
	conn, err := dialDB(ctx, c.dsn) // your dial logic
	if err != nil {
		return nil, err
	}
	_ = conn.Close()
	return &healthcheck.CheckResult{Healthy: true}, nil
}

_ = hc.AddHost("db-primary:5432", healthcheck.WithChecker(&dbChecker{dsn: "db-primary:5432"}))

// Or register a protocol once and select it by name everywhere:
//   checker.Register("redis", factory)  // factory: func(checker.TargetConfig) (checker.Checker, error)
//   hc.AddHost("cache-1:6379", healthcheck.WithProtocol("redis"))
```

Only the root package import is needed — `CheckResult` is re-exported there as a type alias.

### Per-host overrides and three protocols

```go
import (
	healthcheck "github.com/shengyanli1982/go-healthcheck"
	"github.com/shengyanli1982/go-healthcheck/checker"
	_ "github.com/shengyanli1982/go-healthcheck/checker/grpc"
	_ "github.com/shengyanli1982/go-healthcheck/checker/http"
	_ "github.com/shengyanli1982/go-healthcheck/checker/tcp"
)

hc, _ := healthcheck.New() // globals: 5s interval, 2s timeout

// One manager, three protocols — each host picks its own:
_ = hc.AddHost("10.0.0.1:8080", healthcheck.WithProtocol(checker.ProtocolHTTP)) // GET → 2xx-399
_ = hc.AddHost("10.0.0.2:5432", healthcheck.WithProtocol(checker.ProtocolTCP))  // connect OK
_ = hc.AddHost("10.0.0.3:9090", healthcheck.WithProtocol(checker.ProtocolGRPC)) // SERVING

// Per-host overrides (zero value = inherit the global):
_ = hc.AddHost("10.0.0.4:6379",
	healthcheck.WithProtocol(checker.ProtocolTCP),
	healthcheck.WithHostInterval(10*time.Second),      // probe the standby less often
	healthcheck.WithHostTimeout(500*time.Millisecond), // fail fast on this segment
)
```

### gRPC with TLS and service name

```go
// Protocol-specific knobs (gRPC TLS, service name) are not reachable through
// AddHost — build the checker yourself and hand it over via WithChecker.
gc, err := checker.NewChecker(checker.ProtocolGRPC, checker.TargetConfig{
	Addr:    "10.0.0.3:9090",
	Timeout: 2 * time.Second,
	Extra: map[string]string{
		"service": "my.api.v1", // default "": whole-server health
		"tls":     "true",      // default: plaintext
	},
})
if err != nil {
	panic(err)
}
_ = hc.AddHost("10.0.0.3:9090", healthcheck.WithChecker(gc))
```

SERVING = healthy; NOT_SERVING, NOT_FOUND, UNIMPLEMENTED, and connection failures are unhealthy. Supported `Extra` keys: `service` (string — `""` checks the whole server) and `tls` (`"true"`/`"false"` — default plaintext).

## Concurrency

- **Single-writer probing** — at most one worker probes a given host at any instant (TimerQueue Get–Done idempotent mode); slow hosts never accumulate backlog.
- **Callback ordering** — per host, `StateChangeCallback` invocations are serialized in event order; across hosts they run concurrently with no ordering guarantee. Make the callback concurrency-safe.
- **Fast-return discipline** — a slow callback delays the same host's next flip and occupies a probe worker. Never block inside it.
- **Callback forbidden zone** — calling manager APIs that take internal locks (`Start`/`Stop`/`AddHost`/`RemoveHost`/`Report*`) from inside the callback or the internal-error handler deadlocks; read-only queries (`IsHealthy`/`GetHealthyHosts`/`GetLastResult`/`Hosts`) are whitelisted.
- **Panic isolation** — callback panics are recovered and routed to `WithOnInternalError` (nil default: isolated and dropped); checker panics become `CheckResult.Err`. The library never recovers its own bugs.
- **Any-goroutine calls** — every public method may be called from any goroutine.

## Benchmarks

Measured on Apple M1 Max, go 1.23, darwin/arm64, `-benchtime=2s`:

```
BenchmarkProbeFn_FullCycle/steady-10     515.4 ns/op     272 B/op    4 allocs/op
BenchmarkProbeFn_FullCycle/flip-10       523.1 ns/op     272 B/op    4 allocs/op
BenchmarkIsHealthy/hosts-1000-10         24.12 ns/op       0 B/op    0 allocs/op
BenchmarkReportSuccess_SteadyState-10    22.39 ns/op       0 B/op    0 allocs/op
BenchmarkGetHealthyHosts/all-up-10       95.05 µs/op   16384 B/op    1 allocs/op
```

Run them yourself:

```bash
go test -bench=. -benchmem ./...   # 39+ benchmarks across 8 packages
```

Cost notes:

- **Reads and passive reports never allocate.** `IsHealthy` over 1000 hosts costs 24 ns / 0 allocs; a passive report 22 ns / 0 allocs. Zero-alloc guard tests fail the build on any regression.
- **The full probe cycle's 4 allocs all come from `context.WithTimeout`** — the per-probe timeout budget. The library body itself is zero-alloc; the cycle went from 6 to 4 allocs after eliminating hot-path boxing (Scheduler interface `string`→`any` plus one-time host boxing).
- **`GetHealthyHosts` allocates once for 1000 hosts** — the snapshot slice itself. It is a control-plane API; cache from callbacks on the hot path.
- **HTTP probes with bodies dropped −60.8% ns/op and −47% allocs.** Bounded draining (drain up to 4 KB before Close) returns connections to the idle pool instead of discarding them.
- **The ceiling is architectural.** After four pprof rounds closed with no new hotspots, throughput tops out at the upstream workqueue's single mover goroutine (~134k probes/s). Re-evaluate when production approaches ~10k probes/s.

## Comparison

The closest Go alternative is `djylb/upstream`. The differences are deliberate:

| Dimension             | djylb/upstream                               | go-healthcheck                                         |
| --------------------- | -------------------------------------------- | ------------------------------------------------------ |
| Scope                 | full backend pool: LB, retry, routing       | pure health-checking — facts, not decisions            |
| Determinism           | random jitter                                | deterministic spread; `math/rand` banned codebase-wide |
| Protocol extension    | no registration mechanism                    | three layers: Checker interface / Registry / instance  |
| gRPC health protocol  | not built in                                 | built-in subpackage, dependency-isolated               |

## Verification

The v1 delivery is locked by 27 DoD assertions, re-runnable in one shot:

```bash
bash scripts/audit.sh   # any FAIL → non-zero exit
```
