# Gatewright

Gatewright is a readable reverse-proxy API gateway with pluggable rate limiting, built for backend developers who want one static binary, a YAML file they can edit under pressure, and a limiter plugin contract they can extend in Go.

## Install

Build from source with Go 1.26 or newer:

```
go install gatewright/cmd/gatewright@latest
```

or, from a checkout of this repository:

```
go build -o gatewright.exe ./cmd/gatewright
```

The result is a single static binary (`gatewright`) with the admin dashboard and all six limiter strategies embedded. No runtime dependencies, no plugins directory, no service database.

Commands:

| Command | Purpose |
| --- | --- |
| `gatewright run -c gateway.yaml` | Start the gateway (Ctrl+C drains and exits). |
| `gatewright validate -c new.yaml` | Validate config; exits 1 on error, prints every problem found. |
| `gatewright validate -c new.yaml --diff old.yaml` | Validate and print the change set against an older config. |
| `gatewright demo-upstream -a 127.0.0.1:9001` | Run the synthetic demo upstream. |
| `gatewright version` | Print version. |

## Quickstart demo

The following was executed verbatim on Windows PowerShell from a fresh build of this repository. A POSIX shell equivalent follows each step.

Build and start both processes:

```powershell
go build -o gatewright.exe ./cmd/gatewright

$env:GATEWRIGHT_ADMIN_TOKEN = "dev-token-123"

Start-Process -FilePath ".\gatewright.exe" `
  -ArgumentList "demo-upstream","-a","127.0.0.1:9001" -WindowStyle Hidden
Start-Process -FilePath ".\gatewright.exe" `
  -ArgumentList "run","-c","examples/demo/gateway.yaml" -WindowStyle Hidden
```

```bash
go build -o gatewright ./cmd/gatewright

export GATEWRIGHT_ADMIN_TOKEN=dev-token-123

./gatewright demo-upstream -a 127.0.0.1:9001 &
./gatewright run -c examples/demo/gateway.yaml &
```

`examples/demo/gateway.yaml` proxies `:8090` to the demo upstream on `:9001`, exposes the admin server on `127.0.0.1:9901`, and applies one token-bucket limiter (`limit: 20`, `window: 1m`, `burst: 8`) keyed by client IP.

Verify a request succeeds:

```powershell
curl.exe -s -o NUL -w "%{http_code}`n" http://127.0.0.1:8090/users/42
# 200
```

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8090/users/42
# 200
```

Exhaust the bucket (burst capacity is 8) and watch denials begin:

```powershell
1..12 | ForEach-Object { curl.exe -s -o NUL -w "%{http_code} " http://127.0.0.1:8090/users/42 }
# 200 200 200 200 200 200 200 200 429 429 429 429
```

```bash
for i in $(seq 1 12); do curl -s -o /dev/null -w "%{http_code} " http://127.0.0.1:8090/users/42; done
# 200 200 200 200 200 200 200 200 429 429 429 429
```

Observed against a freshly started gateway. The bucket refills continuously at `limit`/`window` (here 1 token per 3 seconds), so if you pause or repeat steps the exact 200-to-429 split point can shift by a request or two.

A denied response carries the full reporting set (observed output):

```
HTTP/1.1 429 Too Many Requests
RateLimit-Limit: 8
RateLimit-Remaining: 0
RateLimit-Reset: 24
Retry-After: 3

{"error":{"code":"RATE001","message":"quota exceeded for key \"ip\\x00127.0.0.1\" (ip-quota)","req_id":"gw-..."}}
```

The legacy aliases `X-RateLimit-Limit/Remaining/Reset` are emitted alongside the modern headers.

Open the dashboard at <http://127.0.0.1:9901/admin/>. Note on auth: when `GATEWRIGHT_ADMIN_TOKEN` is set, every admin request — including the dashboard's own asset and data requests — requires `Authorization: Bearer dev-token-123`; browsers do not send that header, so for pure local browsing you can leave the variable unset (the loopback bind permits unauthenticated admin) or use a REST client that sets it.

Query the admin API with the token:

```powershell
curl.exe -s -H "Authorization: Bearer $env:GATEWRIGHT_ADMIN_TOKEN" http://127.0.0.1:9901/admin/api/state
```

Stop everything when finished:

```powershell
Get-Process gatewright | Stop-Process
```

```bash
kill %1 %2
```

## Configuration reference

YAML. snake_case keys; durations always carry unit suffixes (`30s`, `100ms`); sizes use decimal or IEC suffixes (`512B`, `16KiB`, `2MiB`, `1GiB`). A bare number where a duration or size is expected is a validation error, not an implicit seconds assumption. Unspecified keys take the stated default. Secrets are referenced by environment variable or file, never embedded.

### `version`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `version` | int | `1` | Schema version. |

### `server`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `listen` | string (host:port) | `":8080"` | Gateway listener address. |
| `read_timeout` | duration | `30s` | Maximum time reading a full request. |
| `write_timeout` | duration | `60s` | Declared but note: response writes are not bounded by a fixed write budget so streaming responses outlive it. |
| `idle_timeout` | duration | `120s` | Keep-alive idle timeout between requests. |
| `max_header_bytes` | size | `1MiB` | Cap on the request header block. |

### `server.tls`

Omit the whole block for plain HTTP.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `cert_file` | file path | (empty) | TLS certificate chain; required with `key_file` when TLS is used. |
| `key_file` | file path | (empty) | TLS private key. |
| `min_version` | enum: `tls12`, `tls13` | `tls12` | TLS 1.0/1.1 are rejected by validation; the gateway refuses to ship them. |

### `admin`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `listen` | string (host:port) | `"127.0.0.1:9901"` | Admin API + dashboard listener. Non-loopback binds require auth (validation enforces). |
| `dashboard` | bool | `true` | Serve the embedded UI under `/admin/`. |
| `auth.token_env` | env var name | `GATEWRIGHT_ADMIN_TOKEN` | Variable holding the bearer token. If neither `token_env` nor `token_file` is set, `GATEWRIGHT_ADMIN_TOKEN` is honoured automatically as a fail-safe default. |
| `auth.token_file` | file path | (empty) | Alternative token source; file contents are trimmed and used as the bearer token. |

### `observability.access_log`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | Emit one access-log line per request. |
| `format` | enum: `json`, `human` | `json` | `human` is single-line aligned text, colour only when stdout is a TTY. |
| `output` | enum: `stdout`, `stderr`, file path | `stdout` | Where lines are written. |
| `fields` | string list | `[]` (all fields) | Subset of: `ts req_id method path query route upstream upstream_addr status bytes_in bytes_out duration_ms remote code limiter_name limiter_outcome`. |

### `observability.metrics` and `observability.trace`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `metrics.enabled` | bool | `true` | Expose Prometheus text format on the gateway listener. |
| `metrics.path` | string (path) | `/metrics` | Reserved path serving metrics. |
| `trace` | bool | `false` | Enable per-request pipeline tracing via `X-Gatewright-Trace: 1`. |

### `store`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `path` | file path | (empty) | bbolt database backing shared limiter state. Required when any route's limiter uses `backend: shared`. The file is exclusively locked; exactly one writer transaction runs at a time, across processes. |

### `upstreams.<pool>`

`<pool>` is any name; routes reference it in `upstreams:`.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `targets[].url` | URL | required | e.g. `http://127.0.0.1:9001`. At least one target required. |
| `targets[].weight` | int | `1` | Weighted picking; `0` means the target is never picked. |
| `load_balance` | enum: `round_robin`, `least_connections`, `ring_hash` | `round_robin` | Picker strategy. |
| `hash_key` | string | (empty) | Consistent-hash input selector for `ring_hash`. |
| `connect_timeout` | duration | `5s` | Per-attempt TCP connect budget. |
| `read_timeout` | duration | `30s` | Per-attempt response-read budget. |
| `write_timeout` | duration | `30s` | Per-attempt request-write budget. |
| `keepalive` | duration | `30s` | Idle keep-alive for pooled connections. |
| `max_idle_conns_per_host` | int | `32` | Connection-pool bound per target host. |
| `verify_upstream_tls` | bool | `true` | Upstream certificate verification. Setting `false` logs a loud warning at startup. |

### `upstreams.<pool>.health_check.active`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Periodic active probes. |
| `interval` | duration | `10s` | Probe period. |
| `timeout` | duration | `2s` | Per-probe timeout. |
| `path` | string (path) | required when enabled | Probe target path, e.g. `/healthz`. |
| `method` | HTTP method | `GET` | Probe method. |
| `healthy_threshold` | int | `2` | Consecutive successes to mark healthy. |
| `unhealthy_threshold` | int | `3` | Consecutive failures to mark unhealthy. |
| `verify_tls` | bool | `true` | Certificate verification of probe connections. |

### `upstreams.<pool>.health_check.passive`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `window` | duration | `30s` | Failure-counting window. |
| `failures` | int | `5` | Failures within the window eject the target. |
| `ejection_time` | duration | `30s` | How long an ejected target stays out of rotation. |

### `upstreams.<pool>.circuit_breaker`

Per target. States: closed -> open after `failures` consecutive failures within `window` -> half-open after `cooldown` -> closed again once `half_open_probes` trial requests succeed.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `failures` | int | `10` | Consecutive failures before opening. |
| `window` | duration | `60s` | Failure counting window. |
| `cooldown` | duration | `30s` | Open-to-half-open delay. |
| `half_open_probes` | int | `3` | Trial requests admitted while half-open. |

### `routes[]`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `name` | string | required, unique | Identity used verbatim in logs, dashboard, metrics labels. |
| `hosts` | string list | optional | Exact or `*.wildcard` host predicate. Omitted = any host. |
| `path_prefix` | string | optional | Segment-aligned prefix predicate, e.g. `/v1/`. |
| `path_pattern` | string | optional | Pattern with `{param}` segments, e.g. `/users/{id}`. |
| `methods` | HTTP method list | optional | Omitted = any method. |
| `match_headers[]` | list of `{name, value}` | `[]` | Header predicates; `value` is a regex, empty means presence only. |
| `upstreams` | pool name | required | Pool that serves this route. |
| `strip_prefix` | bool | `false` | Remove the matched `path_prefix` before forwarding. |
| `timeout` | duration | `60s` | Total per-request deadline across the whole route. |
| `body_limit` | size or `"unlimited"` | `32MiB` | Request-body cap; larger bodies are rejected with `BODY001` / 413. |
| `mirror.upstreams` | pool name | optional | Fire-and-forget shadow traffic pool. |
| `mirror.percentage` | float, > 0 and <= 100 | optional | Fraction of requests mirrored. |
| `request_headers.set` | map name->value | `{}` | Overwrite (or add) on the inbound request. |
| `request_headers.add` | map name->value | `{}` | Append without overwriting existing values. |
| `request_headers.del` | string list | `[]` | Delete from the inbound request. |
| `response_headers.set/add/del` | as above | `{}` / `[]` | Same manipulation applied to the upstream response. |

#### `routes[].cors`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `allowed_origins` | string list, or `["*"]` | required when cors present | Origins permitted by preflight and response. |
| `allowed_methods` | method list | required when cors present | e.g. `[GET, POST, OPTIONS]`. |
| `allowed_headers` | string list | `[]` | Request headers permitted in preflight. |
| `expose_headers` | string list | `[]` | Response headers exposed to browser JS. |
| `allow_credentials` | bool | `false` | Sets `Access-Control-Allow-Credentials`. |
| `max_age` | duration | `0` | Preflight cache lifetime. |

CORS runs before auth so preflight `OPTIONS` never needs credentials.

#### `routes[].auth`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `type` | enum: `none`, `api_key`, `jwt` | `none` (no auth middleware attached unless configured) | |
| `api_key.header` | header name | required for api_key | e.g. `X-API-Key`. |
| `api_key.keys_env` | env var name | required (or `keys_file`) | Comma/newline-separated key list. |
| `api_key.keys_file` | file path | — | Alternative key source. |
| `jwt.secret_env` | env var name | for HS* algorithms | HMAC signing secret reference. |
| `jwt.jwks_url` | URL | for RS*/ES* algorithms | JWKS endpoint. |
| `jwt.issuer` | string | optional | Expected `iss` claim. |
| `jwt.audience` | string | optional | Expected `aud` claim. |
| `jwt.algorithms` | algorithm list | required | Subset of `HS256 HS384 HS512 RS256 RS384 RS512 ES256 ES384 ES512`. |

Failures answer `AUTH001` (401, missing/invalid) or `AUTH002` (403, authenticated but not permitted).

#### `routes[].rate_limits[]`

All limiters on a route are ANDed: every one must admit. The most restrictive decision writes the `RateLimit-*` / `Retry-After` headers.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `name` | string | required, unique within route | Metrics/dashboard identity. |
| `strategy` | enum: `fixed_window`, `sliding_window_log`, `sliding_window_counter`, `token_bucket`, `leaky_bucket`, `concurrency` | required | See comparison table below. |
| `key` | key selector | required | See syntax below. |
| `limit` | int >= 1 | required | Units per window (or refill rate for token_bucket, drain rate for leaky_bucket, live count for concurrency). |
| `window` | duration | required except `concurrency` | Window width, >= 1ms. |
| `burst` | int | `limit` | token_bucket bucket capacity. |
| `capacity` | int | `limit` | leaky_bucket queue depth; concurrency live-slot count. |
| `cells` | int | `10` | sliding_window_counter sub-windows; higher is smoother. |
| `max_keys` | int | `65536` | Memory bound on distinct keys per limiter; LRU eviction beyond it increments eviction counters. |
| `backend` | enum: `memory`, `shared` | `memory` | State driver; see below. |

### Route matching precedence

When routes overlap, the winner is deterministic:

1. Host exact match beats no-host; more host predicates beat fewer.
2. Path specificity: static segments > parameter segments > prefix-only matches; longer static prefixes beat shorter.
3. Method-restricted beats method-unrestricted.
4. Header predicates: more constraints beat fewer.
5. Ties resolve by configuration order (first listed wins).

With `observability.trace: true`, the winning route and stage timings are reported via `X-Gatewright-Trace`.

## Limiter strategies

| Strategy | Accuracy | Memory per key | Notes |
| --- | --- | --- | --- |
| `fixed_window` | Allows up to 2x limit across a window boundary burst | ~24 B | Cheapest; boundary burst is inherent. |
| `sliding_window_log` | Exact | ~8 B x events in window x overhead (~48 B/event) | Most accurate, heaviest; events per key bounded via limit only. |
| `sliding_window_counter` | Approximate (+/- limit/cells typical) | ~32 B | Good middle ground; `cells` tunable. When denied by cost > 1, `Remaining` reports usable units honestly rather than forcing 0. |
| `token_bucket` | Exact refill semantics; sustained rate + bursts | ~48 B | Refill `limit`/`window`; burst capacity configurable via `burst`. |
| `leaky_bucket` | Exact drain semantics; smooths output rate | ~48 B | Capacity bounds queue-like bursts. |
| `concurrency` | Exact live count (per process, or per shared store) | ~16 B + waiter set | Admitted units are released when the request completes. |

Memory is bounded per limiter by `max_keys` (default 65536); eviction is LRU. Unbounded key growth is a rejected design.

### Key selectors

`key:` picks the quota bucket per request:

| Selector | Meaning |
| --- | --- |
| `ip` | Client IP from the connection remote address. |
| `path` | Request path. |
| `api_key` | Authenticated API key (requires `auth.type: api_key` upstream in the chain). |
| `header:<NAME>` | Value of the named request header, e.g. `header:X-API-Key`. |
| `composite[a,b,...]` | Tuple of the above, max 4 parts, e.g. `composite[ip,api_key]`. Parts are joined collision-safely. |

### Backends: `memory` vs `shared`

- `memory` (default): state lives in a sharded in-process map with exact LRU and TTL. Fastest; counts only what one gateway process sees. On hot reload, unchanged limiters carry their state over, so a config edit never grants fresh quota mid-window.
- `shared`: state lives in a bbolt database (`store.path`), read-modify-written inside single transactions. bbolt serialises writers through an exclusive OS-level file lock, so multiple gateway processes sharing one file cannot interleave updates and lost updates are impossible without extra locking. Both drivers run the identical strategy logic and pass the same conformance suite, including high-concurrency never-over-admit.

## Writing a custom limiter

A strategy is pure logic: no locks, no clocks beyond the supplied `now`, no I/O. Concurrency belongs to one of two audited drivers inside the engine. To add a strategy named `daily_window` (one counter per UTC day), create `internal/limiter/dailywindow/dailywindow.go`:

```go
// Package dailywindow implements the "daily_window" strategy: a per-key
// counter that resets at each UTC midnight.
package dailywindow

import (
	"encoding/binary"
	"errors"
	"time"

	"gatewright/internal/limiter"
)

const StrategyName = "daily_window"

type Config struct{ Limit int64 }

type state struct {
	Day  int64 // absolute UTC day number
	Count int64
}

const (
	stateVersion = 1
	stateSize    = 1 + 8 + 8 // version + day + count
)

func dayOf(now time.Time) int64 {
	return now.UTC().Truncate(24 * time.Hour).Unix() / 86400
}

// Step is pure: time arrives, decisions and state leave.
func (a Config) Step(prev []byte, existed bool, now time.Time, cost int64) ([]byte, limiter.Decision) {
	var s state
	if existed {
		s, _ = decode(prev)
	}
	today := dayOf(now)
	if s.Day != today {
		s.Day, s.Count = today, 0
	}
	midnight := time.Unix((today+1)*86400, 0).UTC()
	resetIn := midnight.Sub(now)

	d := limiter.Decision{
		Limit:     a.Limit,
		ResetIn:   resetIn,
		Remaining: max(0, a.Limit-s.Count),
	}
	if s.Count+cost <= a.Limit {
		s.Count += cost
		d.Allowed = true
		d.Remaining = a.Limit - s.Count
		return encode(s), d
	}
	d.Remaining = 0
	d.RetryAfter = resetIn // first slot frees at midnight UTC
	return encode(s), d
}

// TTL bounds untouched state survival: the rest of today plus margin.
func (a Config) TTL() time.Duration { return 48 * time.Hour }

func encode(s state) []byte {
	b := make([]byte, stateSize)
	b[0] = stateVersion
	binary.LittleEndian.PutUint64(b[1:9], uint64(s.Day))
	binary.LittleEndian.PutUint64(b[9:17], uint64(s.Count))
	return b
}

func decode(b []byte) (state, bool) {
	if len(b) != stateSize || b[0] != stateVersion {
		return state{}, false
	}
	return state{
		Day:   int64(binary.LittleEndian.Uint64(b[1:9])),
		Count: int64(binary.LittleEndian.Uint64(b[9:17])),
	}, true
}

// Factory builds the engine around the strategy; settings arrive validated.
func Factory(p limiter.Params) (limiter.Limiter, error) {
	if p.Settings.Limit < 1 {
		return nil, errors.New("daily_window: limit must be >= 1")
	}
	return limiter.NewEngine(StrategyName, Config{Limit: p.Settings.Limit}, p), nil
}

// Checker returns human-readable validation problems (empty slice = valid).
func Checker(s limiter.Settings) []string {
	var probs []string
	if s.Limit < 1 {
		probs = append(probs, "limit must be >= 1")
	}
	return probs
}

func init() {
	limiter.Register(StrategyName, Factory)
	limiter.RegisterChecker(StrategyName, Checker)
}
```

Import it once from the binary (add a blank import next to `_ "gatewright/internal/limiter/builtin"` in `cmd/gatewright/run.go`) and `strategy: daily_window` becomes available everywhere, including `validate`.

### Decision invariants

Every Decision your strategy returns must satisfy:

1. `Limit >= 1` always.
2. `Remaining` in `[0, Limit]`; never negative.
3. Denied implies `Allowed == false` and `RetryAfter > 0`; allowed implies `RetryAfter == 0`.
4. `ResetIn >= 0`.
5. Never over-admit under concurrency: racing callers at one instant admit exactly Limit units, never more.
6. Keys never share quota; `cost` consumes exactly `cost` units.

The engine clamps some violations defensively (negative remaining floors to 0, missing retry-after floors to 1ms, a non-positive Limit is converted into a denial) but a strategy relying on those clamps will fail the suite.

### Conformance requirement

Every strategy must pass `internal/limiter/conformance.RunSuite(t, name, make)` on both drivers. The suite drives the public surface with deterministic instants and asserts: decision contract, concurrent never-over-admit (64 goroutines), remaining monotonicity at frozen instants, boundary recovery past ResetIn, key isolation, and exact cost accounting. See `internal/limiter/conformance/conformance.go` and the existing strategy tests for the invocation pattern.

## Middleware order

Fixed, enforced in code, visible in trace mode:

1. **panic recovery** — implicit outermost safety net; converts panics to INT500 envelopes instead of dropped connections.
2. **request-id** — assigns/propagates `X-Gatewright-Request-Id` so every later layer and log line correlates.
3. **access logging** — starts the timer and captures fields, emits after the inner handler completes, including panics and errors.
4. **body size limit** — rejects oversized bodies before auth work is spent on them.
5. **total route timeout** — bounds the whole request lifetime via context deadline.
6. **CORS** — runs before auth so preflight OPTIONS never needs credentials.
7. **auth** — API key/JWT verification populates the identity used by downstream key extractors.
8. **request header manipulation** — set/add/del on the inbound request before limiting and proxying see it.
9. **rate limiting** — all configured limiters ANDed; first denial short-circuits; most restrictive decision reports headers.
10. **routing/proxy** — forwarding with connect/read/write timeouts, retries, and the circuit breaker.

Retries apply to idempotent methods only (GET, HEAD, OPTIONS, PUT, DELETE, TRACE), never once the response has started streaming, and bodies replay only when buffered (<= 8 KiB). Backoff is base 25ms doubling with +/-50% uniform jitter, attempts default 3 (1 retry).

## Metrics reference

Served at `/metrics` (gateway listener) in Prometheus text exposition v0.0.4. Naming: `gatewright_<noun>_<unit>`; counters end `_total`; histograms end `_seconds`; gauges carry no unit suffix.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `gatewright_limiter_decisions_total` | counter | `name`, `strategy`, `outcome` (`allowed`\|`limited`) | Rate limiter admission decisions. |
| `gatewright_reloads_total` | counter | `outcome` (`applied`\|`rejected`) | Configuration reload attempts (file-watch, admin API). |
| `gatewright_upstream_requests_total` | counter | `pool`, `target`, `code` | Requests sent to upstream targets, by response status code. |
| `gatewright_upstream_healthy` | gauge | `pool`, `target` | Target accepting traffic (1) or not (0). |
| `gatewright_circuit_state` | gauge | `pool`, `target` | Circuit breaker state: 0 closed, 0.5 half-open, 1 open. |
| `gatewright_upstream_request_duration_seconds` | histogram | `pool`, `target` | Upstream attempt latency. |

Histogram buckets default to 1ms..10s in seconds: `0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10`, plus implicit `+Inf`, with `_sum` and `_count` series.

Reserved paths on the gateway listener: `/healthz` (liveness JSON), `/readyz` (503 listing pools without healthy targets until ready), `/metrics` (when enabled).

## Admin API and dashboard

Bound to `admin.listen` (default `127.0.0.1:9901`). All endpoints share one auth model:

- With a token configured (via `auth.token_env`, `auth.token_file`, or the automatic `GATEWRIGHT_ADMIN_TOKEN` fail-safe), every request needs `Authorization: Bearer <token>`. Comparison is constant-time over SHA-256 digests. Missing/invalid tokens get AUTH001 (401).
- With no token configured, the bind must be loopback (validation enforces this) and requests pass unauthenticated.

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/admin/api/state` | GET | Full snapshot JSON: version, uptime, routes (rps, p50/p95/p99), pools/targets with health and circuit words, limiter views. |
| `/admin/api/reload` | POST | Reload configuration from disk; failing candidates never touch the running generation. |
| `/admin/api/metrics` | GET | Current Prometheus text exposition. |
| `/admin/events` | GET | Server-sent events stream pushing `state` frames (~1s cadence). |
| `/admin/` | GET | Embedded dashboard UI (when `dashboard: true`); `/admin` redirects here. |
| `/admin/assets/*` | GET | Dashboard styles/scripts. |

Errors use the same envelope as the proxy: `{"error":{"code","message"}}`.

Dashboard specifics:

- Themes: dark (default), light, high-contrast — cycled with the theme button; choice persists locally.
- Keyboard: `j`/`k` move row selection, `Enter` opens the route detail pane, `Esc` closes detail or overlay, `/` focuses the filter box, `?` shows the shortcut overlay.
- Accessibility: full keyboard operability, visible focus rings, AA contrast including chart axis labels, tabular figures with fixed column widths so live refreshes cause zero layout shift, `prefers-reduced-motion` respected globally (transitions disabled entirely), health shown as dot + word (never colour alone), screen-reader status announcements, skeleton rows marked `aria-busy`, em dash placeholders for metric gaps rather than fabricated zeros.
- Latency chart: rolling 5-minute window, 10s buckets, three named series p50/p95/p99, linear y-axis starting at 0, raw values (no smoothing).

## Architecture

Request lifecycle for one proxied call:

1. The gateway listener accepts the connection (server-level read/idle timeouts apply; TLS terminates here if configured).
2. Reserved operational paths (`/healthz`, `/readyz`, `/metrics`) are served locally and never routed.
3. The fixed outer chain runs: panic recovery wraps everything, request-id assigns the correlation id, access logging starts its timer.
4. The dispatcher asks the router for a match using the documented precedence; unmatched traffic is denied RT001 (404) — fail closed, no default route. Method conflicts answer RT002 (405) with an Allow header.
5. The matched route's lazily-assembled chain stages run in order: body-limit, total-timeout, CORS, auth, request-headers, rate-limit (each timed for trace mode).
6. The forwarder attempts the request: retries idempotent methods with jittered backoff while the breaker is closed and the body is replayable, mapping failure modes to UP00x codes.
7. The pool picker chooses a healthy, weighted target (round-robin, least-connections, or ring hash) and the request goes out over pooled keep-alive connections; passive health tracking and the circuit breaker observe the outcome.
8. Mirror routes additionally fire a fire-and-forget copy at the mirror pool by percentage.
9. Access logging emits the completed line; concurrency limiter slots are released exactly once at response completion.

Reload lifecycle: the supervisor watches the config file mtime (500ms poll) and also accepts POST `/admin/api/reload`. Each reload builds a complete new immutable generation and swaps it in atomically; the old generation stops background work and drains in-flight requests up to a 30s deadline before closing. A candidate that fails load or build never touches the running generation. On shutdown (SIGINT/SIGTERM) both listeners drain gracefully with a 30s deadline, then pools close and the shared store releases its file lock.

## Testing

All suites run with the Go toolchain from the repository root.

```
# Unit tests, all packages
go test ./...

# Limiter conformance suite specifically (every strategy x both drivers)
go test ./internal/limiter/...

# Race detector across everything (recommended before releases)
go test -race ./...
```

Race detector on Windows requires a C toolchain (mingw-w64 gcc) and cgo. Put mingw on PATH first, then run:

```powershell
$env:PATH = "C:\mingw64\bin;$env:PATH"   # adjust to your mingw-w64 install
$env:CGO_ENABLED = "1"
go test -race ./...
```

End-to-end behaviour is exercised by driving the real binaries the way the quickstart does: `gatewright demo-upstream` plus `gatewright run -c examples/demo/gateway.yaml`, then asserting status codes, headers, reserved paths, and reload behaviour over HTTP. The demo config is the canonical fixture.

Load generation lives in `test/load`, stdlib-only, built separately:

```
go build -o load ./test/load
```

Usage (`load -h` prints the full flag set):

```
load -url http://127.0.0.1:8080/v1/ -d 30s -c 8
load -url http://127.0.0.1:8090/users/1 -qps 200 -header "X-API-Key: k1" -output json
```

Key flags: `-url` (required), `-method`, `-c` workers (default 8), `-d` duration (default 30s), `-qps` target rate (0 = tight loop), `-timeout` per request, `-body` bytes, `-header` repeatable, `-expect-status` (default 200), `-output human|json`, `-allow-errors`. Exit code is 1 when the error fraction exceeds 5% unless `-allow-errors` is passed. Results report honest counts and histogram-interpolated p50/p90/p95/p99.

Benchmarks: none are published in this README because none have been measured on reference hardware. Run `test/load` against your own deployment and publish your own numbers.

## License

MIT — see [LICENSE](LICENSE).
