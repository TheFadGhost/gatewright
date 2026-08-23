# DESIGN.md

This document is the contract for how Gatewright looks, behaves, and is extended.
It is written **before** feature code. Feature code is reviewed against it.

---

## Point of view

Gatewright is an operations tool that tells the truth quickly. Dense, precise, unexcited:
every screen answers "what is happening right now, and is it wrong?" without decoration,
and every log line carries enough identity to be greppable at 3am. Numbers are shown
with their units, percentiles with their names, health with a word — never colour alone.
If a display element does not help an operator decide something, it does not exist.
We reject the SaaS-dashboard look deliberately: no gradient heroes, no glassmorphism,
no animated counters, no stat cards floating on shadows. The dashboard is a control
surface; the config file is a human interface; the plugin API is a promise. All three
are designed, not defaulted.

---

## 1. The limiter plugin interface

**Design principle: the interface must make a correct implementation natural and an
incorrect one awkward.**

The levers that enforce this:

1. **Time is supplied, never read.** `Allow` receives `now`. An implementation cannot
   call `time.Now()` mid-algorithm, which eliminates hidden clock reads, makes
   window-boundary tests deterministic, and centralises monotonic-time handling.
2. **Cost is explicit.** Weighted requests pass `cost`; nothing can silently assume 1
   and then miscount when someone sends `cost: 5`.
3. **The decision is total.** `Decision` carries limit, remaining, retry-after, reset.
   There is no way to return a bare bool; the conformance suite asserts every field,
   so an inaccurate implementation fails tests instead of lying in production.
4. **State handling exists exactly twice.** Every algorithm is written once as pure
   logic over a small state struct (`step`). Two audited drivers (in-memory, shared
   store) own all concurrency. Six copies of subtly-different locking cannot exist.

### The interface, as an implementer types it

```go
// Package limiter defines the rate-limiting plugin contract.
package limiter

// Decision is the complete outcome of one admission check.
type Decision struct {
    Allowed    bool          // true => forward the request
    Limit      int64         // capacity governing this key's current window/bucket
    Remaining  int64         // units left after this call; >= 0; 0 when denied
    RetryAfter time.Duration // > 0 iff !Allowed; earliest retry that could succeed
    ResetIn    time.Duration // until the window resets / bucket refills fully
}

// Limiter is the entire strategy surface. One method.
type Limiter interface {
    // Allow accounts cost units against key at instant now.
    // Implementations must not read wall clocks; now is authoritative.
    Allow(key string, now time.Time, cost int64) Decision
}

// Releaser is optionally implemented by concurrency-style limiters whose
// admitted units are returned when the request finishes.
type Releaser interface {
    Release(key string, now time.Time, cost int64)
}

// Factory builds a strategy from already-validated settings.
// No string parsing happens inside strategies.
type Factory func(p Params) (Limiter, error)

type Params struct {
    Route    string                 // owning route name (metrics label)
    Name     string                 // limiter instance name from config
    Settings map[string]any         // typed settings block
    Backend  Backend                // nil => in-memory driver
}

// Register installs a factory under its config name ("token_bucket", ...).
func Register(strategy string, f Factory)
```

### Storage drivers (where concurrency lives)

```go
// Backend is a transactional, keyed state store shared across processes.
// Update applies mutate atomically to the blob stored under key.
type Backend interface {
    Update(key string, ttl time.Duration,
        mutate func(prev []byte, existed bool) (next []byte, resp []byte)) ([]byte, error)
}
```

Each algorithm ships as: `state` struct + pure `step(prev *state, now, cost, cfg)
(Decision, error)` + encode/decode + a thin registration of both drivers. The
in-memory driver is a sharded map with per-key mutexes and bounded key space;
the shared-store driver runs the identical `step` inside a `Backend.Update`
transaction (bbolt single-writer MVCC). Both pass the same conformance suite,
including high-concurrency never-over-admit.

### Trivial implementation in full: fixed window

```go
package fixedwindow

import (
    "time"
    "gatewright/internal/limiter"
)

type state struct {
    WindowStart int64 // unix nanos
    Count       int64
}

type Config struct{ Limit int64; Window time.Duration }

func Step(s *state, now time.Time, cost int64, c Config) limiter.Decision {
    start := now.Truncate(c.Window)
    if s.WindowStart != start.UnixNano() { // window rolled over
        s.WindowStart, s.Count = start.UnixNano(), 0
    }
    d := limiter.Decision{
        Limit:   c.Limit,
        ResetIn: c.Window - now.Sub(start),
        Remaining: max(0, c.Limit-s.Count),
    }
    if s.Count+cost <= c.Limit {
        s.Count += cost
        d.Allowed, d.Remaining = true, c.Limit-s.Count
        return d
    }
    d.RetryAfter = d.ResetIn // first slot frees at window rollover
    return d
}

func Factory(p limiter.Params) (limiter.Limiter, error) {
    c, err := configFrom(p.Settings) // validated: Limit >= 1, Window >= 1ms
    if err != nil { return nil, err }
    return limiter.NewEngine(stateSpec{}, c, p), nil
}

func init() { limiter.Register("fixed_window", Factory) }
```

### Non-trivial implementation in full: sliding window counter

Approximates a sliding window over `cells` sub-windows; interpolates the previous
cell's contribution by elapsed fraction. Accuracy/memory trade-off documented below.

```go
package slidingcounter

import (
    "time"
    "gatewright/internal/limiter"
)

type state struct {
    Cells    [2]int64 // [prevCellCount, curCellCount]
    CellIdx  int64    // absolute cell index of Cells[1]
}

type Config struct {
    Limit int64
    Window time.Duration
    Cells int64 // default 10; higher = smoother, more state churn
}

func cellOf(now time.Time, w time.Duration, cells int64) (int64, float64) {
    cw := w / time.Duration(cells)
    abs := now.UnixNano() / int64(cw)
    frac := float64(now.UnixNano()%int64(cw)) / float64(int64(cw))
    return abs, frac
}

func Step(s *state, now time.Time, cost int64, c Config) limiter.Decision {
    abs, frac := cellOf(now, c.Window, c.Cells)
    switch {
    case abs == s.CellIdx+1: // advance one cell
        s.Cells[0], s.Cells[1] = s.Cells[1], 0
        s.CellIdx = abs
    case abs > s.CellIdx+1: // long gap: fresh state
        s.Cells = [2]int64{}
        s.CellIdx = abs
    }
    used := int64(float64(s.Cells[0])*(1-frac)) + s.Cells[1]
    cellWidth := c.Window / time.Duration(c.Cells)
    d := limiter.Decision{
        Limit:     c.Limit,
        Remaining: max(0, c.Limit-used),
        // full reset when the oldest contributing cell ages out completely
        ResetIn:   time.Duration((1-frac)*float64(cellWidth)) + cellWidth*time.Duration(c.Cells-1),
    }
    if used+cost <= c.Limit {
        s.Cells[1] += cost
        d.Allowed = true
        d.Remaining = max(0, c.Limit-(used+cost))
        return d
    }
    // earliest retry: at the next cell boundary the previous cell's weight drops
    d.RetryAfter = max(time.Millisecond, time.Duration((1-frac)*float64(cellWidth)))
    return d
}

func init() { limiter.Register("sliding_window_counter", Factory) }
```

(The engine serialises `state`, runs `Step` under the key's lock or inside a backend
transaction, and enforces the Decision invariants; strategies never touch locks.)

### Strategy trade-offs (authoritative table)

| Strategy | Accuracy | Memory per key | Notes |
| --- | --- | --- | --- |
| fixed_window | Allows up to 2×limit across a window boundary burst | ~24 B | Cheapest; boundary burst is inherent |
| sliding_window_log | Exact | ~8 B × events in window × overhead (~48 B/event) | Most accurate, heaviest; bound events/key via limit only |
| sliding_window_counter | Approximate (±limit/cells typical) | ~32 B | Good middle ground; cells tunable. When denied by cost>1, `Remaining` reports the usable units honestly rather than forcing 0 |
| token_bucket | Exact refill semantics; sustained rate + bursts | ~48 B | Refill limit/window; burst capacity configurable |
| leaky_bucket | Exact drain semantics; smooths output rate | ~48 B | Capacity bounds queue-like bursts |
| concurrency | Exact live count (per process, or per shared store) | ~16 B + waiter set | Must be paired with `Releaser` usage |

All strategies: memory bounded per limiter by `max_keys` (default 65 536). Eviction is
LRU; evictions increment `gatewright_limiter_evictions_total`. Unbounded key growth is
a rejected design.

### RateLimit headers (what the gateway reports)

On every limited route response:

- `RateLimit-Limit: <limit>`
- `RateLimit-Remaining: <remaining>` (accurate post-decision value from Decision)
- `RateLimit-Reset: <seconds>` (ceil of ResetIn)
- On denial additionally: `Retry-After: <seconds>` (ceil of RetryAfter, min 1)

Legacy compatibility aliases `X-RateLimit-Limit/Remaining/Reset` are also emitted.
When multiple limiters apply to a route, the most restrictive decision wins for
reporting; each decision is logged individually.

---

## 2. Configuration file design (a human interface)

YAML. Read by whoever is paged, edited under pressure, so:

- **snake_case keys**, always; enums are bare words (`round_robin`, not `"ROUND_ROBIN"`).
- **Nesting depth ≤ 3** (e.g. `routes[].rate_limits[].window` is the floor).
- **Units are always explicit**: durations are strings with unit suffix (`30s`, `100ms`);
  sizes use IEC suffixes (`16KiB`, `2MiB`). A bare number where a duration/size is
  expected is a validation **error**, not an implicit seconds assumption.
- **Enums, not magic strings**: strategies, formats, load balancers, hash keys, TLS
  versions, auth types are enumerated and validated with named alternatives in errors.
- **Every default is stated** in README's config reference; unspecified ≠ undefined.
- Secrets are referenced, never embedded: `keys_env: GW_API_KEYS`,
  `token_env: GATEWRIGHT_ADMIN_TOKEN`, `secret_env: GW_JWT_SECRET`.

Annotated example (full reference lives in README):

```yaml
version: 1

server:
  listen: ":8080"            # default ":8080"
  read_timeout: 30s          # default 30s
  write_timeout: 60s         # default 60s
  idle_timeout: 120s         # default 120s
  tls:                       # omit for plain HTTP
    cert_file: certs/tls.crt
    key_file: certs/tls.key
    min_version: tls12       # default tls12

admin:
  listen: "127.0.0.1:9901"   # default loopback; non-loopback requires auth
  auth:
    token_env: GATEWRIGHT_ADMIN_TOKEN
  dashboard: true            # default true

upstreams:
  catalog:
    targets:
      - url: "http://127.0.0.1:9001"
        weight: 1            # default 1
    load_balance: round_robin   # round_robin|least_connections|ring_hash
    connect_timeout: 5s
    read_timeout: 30s
    write_timeout: 30s
    health_check:
      active:
        enabled: true
        interval: 10s
        timeout: 2s
        path: "/healthz"
        healthy_threshold: 2
        unhealthy_threshold: 3
      passive:
        window: 30s
        failures: 5           # failures within window eject the target
        ejection_time: 30s
    circuit_breaker:
      failures: 10            # consecutive failures before open
      cooldown: 30s           # open -> half-open delay
      half_open_probes: 3

routes:
  - name: api-v1              # required, unique
    hosts: ["api.example.com"]          # optional predicate
    path_prefix: "/v1/"                 # optional predicate
    path_pattern: "/users/{id}"         # optional predicate (params: {name})
    methods: [GET, POST]                # optional predicate; omitted = any
    upstreams: catalog                  # required pool name
    strip_prefix: true                  # default false
    timeout: 15s                        # per-request total deadline; default 60s
    body_limit: 2MiB                    # default 32MiB; explicit "unlimited" allowed
    rate_limits:
      - name: ip-burst                  # required, unique within route
        strategy: token_bucket          # see strategy table
        key: ip                         # ip|path|api_key|header:X-API-Key|composite[ip,api_key]
        limit: 100                      # required
        window: 1m                      # required unless strategy=concurrency
        burst: 20                       # optional (token_bucket); default = limit
        max_keys: 65536                 # optional; default 65536
        backend: memory                 # memory|shared; default memory
```

Precedence when routes overlap (documented, deterministic):

1. Host exact match beats no-host; more host predicates beat fewer.
2. Path specificity: static segments > parameter segments > prefix-only matches;
   longer static prefixes beat shorter.
3. Method-restricted beats method-unrestricted.
4. Header predicates: more constraints beat fewer.
5. Ties resolve by configuration order (first listed wins).

The winner and the losing candidates are visible via request trace mode.

---

## 3. Error taxonomy and shapes

Stable codes `PREFIX###`. Proxy/API responses share one envelope:

```json
{"error": {"code": "rate_limited", "message": "quota exceeded for key 'ip:10.0.0.7'", "req_id": "gw-01J9..."}}
```

| Code | HTTP | Meaning |
| --- | --- | --- |
| CFG001–CFG006 | — (config load) | unknown field, invalid value, missing required, duplicate name, semantic conflict, unsafe combination |
| RT001 | 404 | no route matched |
| RT002 | 405 | route matched but method disallowed (sets Allow header) |
| AUTH001 | 401 | missing/invalid credentials |
| AUTH002 | 403 | authenticated, not permitted |
| RATE001 | 429 | rate/concurrency limit exceeded (+ Retry-After) |
| BODY001 | 413 | body exceeds route limit |
| UP001 | 504 | upstream connect timeout |
| UP002 | 504 | upstream read timeout |
| UP004 | 504 | total route timeout |
| UP010 | 502 | bad gateway (reset/malformed upstream response) |
| UP011 | 503 | circuit open for upstream |
| UP012 | 503 | no healthy upstream available |
| INT500 | 500 | gateway internal error |

### Config validation error shape

Machine shape (also printed humanly by the CLI):

```json
{
  "file": "gateway.yaml",
  "line": 34, "column": 7,
  "path": "routes[2].rate_limits[0].window",
  "found": "90",
  "expected": "duration string with unit suffix",
  "code": "CFG002",
  "hint": "write \"90s\"; bare integers are rejected so units are never guessed"
}
```

CLI rendering:

```
error[CFG002]: routes[2].rate_limits[0].window: expected duration string with unit suffix, found "90"
  --> gateway.yaml:34:7
  hint: write "90s"; bare integers are rejected so units are never guessed
```

Rules: every malformed case names the exact path; validation collects **all** errors,
not the first; exit code 1; zero output noise on success (`gateway.yaml: OK (2 routes,
1 pool, 6 limiters)`).

---

## 4. Access log format (field names fixed once)

Fields (exact keys, JSON mode):

`ts, level, msg, req_id, method, path, query, route, upstream, upstream_addr, status,
bytes_in, bytes_out, duration_ms, remote, code, limiter_name, limiter_outcome`

JSON mode (one object per line):

```json
{"ts":"2026-08-22T09:14:07.412Z","level":"info","msg":"access","req_id":"gw-01J9ZK...","method":"GET","path":"/v1/users/42","query":"","route":"api-v1","upstream":"catalog","upstream_addr":"127.0.0.1:9001","status":200,"bytes_in":0,"bytes_out":1523,"duration_ms":12.31,"remote":"203.0.113.9:52344","code":"","limiter_name":"ip-burst","limiter_outcome":"allowed"}
```

Human mode (single line, aligned, colour only when TTY):

```
2026-08-22T09:14:07Z INF req_id=gw-01J9ZK GET /v1/users/42 route=api-v1 upstream=catalog status=200 dur=12.3ms out=1523B remote=203.0.113.9
```

Configurable subset via `observability.access_log.fields`; defaults emit all core fields.

---

## 5. Naming conventions (all modules)

- Go packages: lowercase, single word, `internal/<area>`.
- Config keys: snake_case. Env vars: `GATEWRIGHT_*`.
- CLI flags: kebab-case (`--no-color`, `--config`).
- Metrics: `gatewright_<noun>_<unit>`; counters `_total`; histograms `_seconds`;
  gauges carry no unit suffix. Labels lowercase (`route`, `upstream`, `strategy`,
  `outcome`, `code`).
- Headers added by the gateway: `X-Gatewright-*` (request id, trace).
- Error codes: `<AREA><###>` as tabulated above.
- Dashboard/log identifiers reuse config names verbatim (a route called `api-v1`
  appears as `api-v1` everywhere; no re-spelling).

## 6. Middleware chain — fixed order

Documented, enforced in code, visible in trace mode:

1. panic recovery (implicit outermost)
2. request-id (`X-Gatewright-Request-Id`)
3. access logging (starts timer, emits after inner completes)
4. body size limit
5. total route timeout (context deadline)
6. CORS (preflight short-circuit before auth)
7. auth (API key / JWT)
8. request header manipulation
9. rate limiting (all limiters ANDed; most restrictive reports headers)
10. routing/proxy — inside the transport: connect/read/write timeouts,
     retries (idempotent methods only, jittered backoff), circuit breaker per upstream

Retries: GET/HEAD/OPTIONS/PUT/DELETE/TRACE only; never retried once the response has
started streaming to the client; bodies replayable only when buffered ≤ 8 KiB.
Backoff: base 25 ms, factor 2, ±50 % uniform jitter, attempts default 3 (1 retry).

Request trace mode (`observability.trace: true`, request header `X-Gatewright-Trace: 1`):
the `X-Gatewright-Trace` response header reports the matched route plus the stage
timings that completed before the upstream responded (header emission is bound to the
response, so post-forward stages cannot appear there). The complete pipeline — every
stage with inclusive durations in completion order — is emitted as a structured
`request-trace` log event with `req_id`, `route`, `status` and `stages`. Stage timings
are inclusive: an outer stage's duration contains all inner work.

---

## 7. Dashboard visual specification

Register: dense ops console. No marketing surfaces.

### Type & spacing

- Labels/UI text: system sans stack (`Inter, "Segoe UI", system-ui, sans-serif`),
  12–13 px. Numerals/values: monospace stack with `font-variant-numeric: tabular-nums`.
- Type scale: 11 / 12 / 13 / 15 / 20 px. Line-height 1.45.
- Spacing on a 4 px grid: 4, 8, 12, 16, 24, 32. Row height fixed 36 px.
- Page grid: full-width rows, section gutters 16 px, no cards-with-shadows; sections
  separated by 1 px rules.

### Semantic colour tokens

Dark theme is the default; light and high-contrast themes are pure token overrides.

| Token | Dark | Light | High-contrast |
| --- | --- | --- | --- |
| --bg | #0d1117 | #ffffff | #000000 |
| --bg-raised | #161b22 | #f6f8fa | #101418 |
| --border | #30363d | #d0d7de | #3d444d |
| --text | #e6edf3 | #1f2328 | #ffffff |
| --text-dim | #9198a1 (≥4.5:1) | #59636e (≥4.5:1) | #b6bec8 |
| --ok | #3fb950 | #1a7f37 | #4ade80 |
| --warn | #d29922 | #9a6700 | #fbbf24 |
| --down | #f85149 | #cf222e | #ff6b66 |
| --accent | #58a6ff | #0969da | #79c0ff |
| --focus | #58a6ff | #0969da | #ffffff |

Health mapping is **state → label + token**, never colour alone:
`healthy`(ok) / `probing`(warn) / `ejected`(warn) / `down`(down) / `draining`(dim).
Tokens chosen so healthy/down remain distinguishable under deuteranopia (luminance
gap plus mandatory text labels).

### Route row anatomy (fixed height, no shift)

```
│ NAME      MATCH                     UPSTREAM        RPS    P95      STATUS       LIMITER            │
│ api-v1    api.example.com/v1/*      ● catalog       142.1  23 ms    140/2/0      ip-burst 71% ▓▓▓░ │
```

Columns left-to-right: name (bold), match summary (host + path pattern),
upstream chip (dot + label + health word), requests/sec (tabular, fixed 7ch),
p95 latency (named percentile, fixed 7ch), status counts `2xx/4xx/5xx` (fixed 9ch),
limiter name + usage bar (bar is proportional, axis starts at 0, capped label ">100%").

### Upstream health indicator anatomy

`● catalog — healthy` : 8 px dot in state token, target name, state word.
Ejected targets append `(ejected 12s ago)`. Never a dot without its word.

### Latency chart treatment

- Rolling 5-minute window, 10 s buckets.
- Three series: **p50**, **p95**, **p99** — percentiles named in legend, never averages.
- Y-axis linear, starts at 0, ticks labelled with units (ms); x-axis labelled in clock time.
- No smoothing; raw bucket values plotted. Spikes are information.
- Axis labels meet AA contrast against --bg (--text-dim).

### Limiter activity display

Per limiter row: instance name, strategy tag, key type, decisions/s split into
allowed vs limited (stacked bars from 0), current usage %, eviction counter when > 0.

### Motion rules

Default motionless. SSE updates replace values in place. Any permitted transition is
≤ 120 ms opacity-only and disabled entirely under `prefers-reduced-motion: reduce`.
Banned outright: animated count-ups, gauge dials, pulsing indicators.

### States (all four specified)

- Loading: skeleton rows, `aria-busy="true"` on the region.
- Empty: literal sentence ("No routes configured."), centred-left, dim text.
- Error: panel with 3 px --down left border, message, "Retry" button.
- No data (metric gap): em dash placeholder — never a fabricated 0.

### Accessibility requirements (audited)

- Full keyboard operability: Tab through sections, `j`/`k` move row selection,
  `Enter` opens detail pane, `Esc` closes, `/` focuses filter, `?` shows shortcuts.
- Visible focus ring (2 px --focus outline, 2 px offset) on every interactive element.
- `NO_COLOR` env and `--no-color` flag disable colour in terminal output; colour is
  off automatically when stdout is not a TTY. Palette uses standard 16-colour ANSI
  codes so legacy terminals render correctly.
- AA contrast everywhere including chart axis labels.
- `prefers-reduced-motion` respected globally.
- Tabular figures for every numeric column; column widths fixed so live refreshes
  cause zero layout shift.

### Theme policy

Exactly three dashboard themes (dark default, light, high-contrast) as token
overrides, plus two terminal themes (dark/light) and plain no-colour mode. This is an
ops tool; we do not build a theme gallery.

---

## 8. TLS posture

- Listener TLS terminates with `min_version: tls12` default; TLS 1.0/1.1 accepted only
  when explicitly configured, and then warned loudly in logs at startup.
- Upstream verification is ON by default (`verify_upstream_tls: true` implied); a
  config that sets it false produces a startup warning in both log and CLI output —
  there is no silent way to disable it.
- Test/demo certificates are generated at test time by tooling in this repo and are
  never committed.
