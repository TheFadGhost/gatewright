# AUDIT.md

Pre-1.0.0 audit record. Three independent audits were performed by agents that did
not write the audited code: **security**, **code quality**, and **design** (dashboard,
logs, config UX, judged against DESIGN.md). Every finding and its disposition is
listed. "Fixed" commits landed before v1.0.0; the full suite (`go test ./... -race`)
was re-run green after the fix wave.

## Security audit

| Sev | Finding | Disposition |
| --- | --- | --- |
| MED | Encoded dot-segments (`%2e%2e`) forwarded verbatim → upstream-side path confusion defeating gateway authz | Fixed: decoded-segment normalization in router+forwarder; offending paths rejected `400 RT003`; e2e test added |
| MED | Tokenless loopback admin readable via DNS rebinding | Fixed: admin server enforces loopback Host allowlist when no token is configured; spoofed-Host e2e test added |
| MED | Shared limiter backend grew without bound (no expiry sweep) | Fixed: `store.DB.StartSweeper` deletes expired entries per bucket |
| MED | Limiter key length unbounded → memory amplification via giant headers | Fixed during key-extractor hardening: composite/header parts are length-capped before use |
| MED | `token_file` read failure silently degraded to no-auth on loopback | Fixed: configured-but-unresolvable token source fails startup loudly |
| LOW | Header Set/Add values not validated for control chars | Fixed: CFG002 on non-printable values at exact config paths |
| LOW | Dead tls10/tls11 branches contradicted strict validation | Fixed: enum reduced to `tls12\|tls13` everywhere (README/DESIGN/code agree) |
| LOW | RATE001 message echoed attacker-controlled key | Fixed: message names only the limiter instance |
| LOW | req_id read from raw header in error paths | Fixed: context value preferred, header fallback |
| LOW | Reload failures leaked internals to admin clients | Fixed: generic client message, detail logged server-side |
| LOW | `ring_hash.hash_key` validated then ignored (always IP) | Fixed: forwarder extracts the configured key (ip/path/api_key/header/composite) and feeds the picker |
| LOW | Mirror fanout goroutines unbounded under spikes | Accepted for v1: lifetime-bounded (5 s timeout, 1 MiB drain cap, replay-only ≤8 KiB); documented |
| LOW | Demo websocket reader allocated attacker-controlled frame size | Fixed: 1 MiB frame cap, streamed reads |
| LOW | JWKS URL validation accepted `httpfoo://` | Fixed: parsed URL + exact scheme check |
| LOW | `/readyz` disclosed unhealthy pool names publicly | Fixed: opaque `{"status":"unavailable"}` body |

Cleared explicitly: request smuggling (framing never hand-parsed; hop-by-hop set
complete vs RFC 9110 incl. Connection-named fields and the TE:trailers exception),
JWT verification (alg=none impossible via double whitelist, exp required,
HS↔RS confusion blocked twice, untrusted-kid rejected), constant-time API-key and
admin-token comparison, CORS wildcard/credentials guard, request-id sanitisation,
body-limit ordering, goroutine hygiene (SSE broker, timeout watchers, mirror),
secrets hygiene (nothing committed; env/file sources never logged).

## Code-quality audit

| Sev | Finding | Disposition |
| --- | --- | --- |
| HIGH | `INT500` served as HTTP 502 | Fixed: dedicated status mapping |
| HIGH | `strip_prefix` wired in tests only — dead in production | Fixed: passed through `buildRuntime` + e2e test |
| HIGH | Traffic mirroring unreachable from production wiring | Fixed: `Mirror` passed to forwarder; guarded e2e covers it |
| HIGH | `response_headers` validated but never applied | Fixed: `NewResponseHeaders` stage inserted into every route chain |
| HIGH | Access-log `route` always `"<gateway>"` | Fixed: dispatcher enrichment wins |
| MED | `upstream`/`upstream_addr` log fields never populated | Fixed: forwarder stamps the request record |
| MED | RT002 never carried the `Allow` header | Fixed: allow-list returned structurally from the router |
| MED | UP001–UP003 unreachable; all timeouts mapped to UP004 | Partially fixed: connect-timeout classification distinct where the transport reports it; residual collapsed cases documented in errs comments |
| MED | NaN `mirror.percentage` accepted → mirrored 100 % | Fixed: NaN/Inf rejected at load |
| MED | Shared-store update failure silently denied traffic as RATE001 | Fixed: deliberate fail-CLOSED policy with RetryAfter, one-shot warning, store-error metric |
| MED | Auth misconfig panicked on first request after reload | Fixed: chains built eagerly inside reload; panics become build errors rejecting the candidate |
| MED | Drain accounting race (`wg.Add` vs `Wait`) | Fixed: generation RWMutex + atomic refs protocol; in-flight-across-reload test pins it |
| MED | In-memory limiter globally serialized despite sharding | Fixed: per-shard mutexes |
| MED | `access_log` config ignored by `run` | Fixed: logger built from loaded config |
| MED | Log-field vocabulary duplicated in two packages | Fixed: single source in obs, aliased by config |
| MED | Old bbolt handle leaked when `store.path` changed across reloads | Fixed: path changes rejected with explicit error (documented) |
| MED | ReverseProxy aborts logged as gateway panics | Fixed: `http.ErrAbortHandler` special-cased |
| LOW | Dead exported symbols (NextSeq, Clock trio, Settings field, writeErr, …) | Removed |
| LOW | Eviction telemetry promised but never emitted | Fixed: engine fires `ObserveEviction`; dashboard counter live |
| LOW | Drain doc/comment mismatch | Fixed comment to match behaviour |
| LOW | Client-cancel counted against breaker/passive health | Fixed: `ErrCanceled` outcomes ignored by target state |
| LOW | bbolt bucket-name collisions via `/`→`_` sanitizer | Fixed: route/limiter names restricted to `[A-Za-z0-9_-]{1,64}` |
| LOW | Naming drift (`strategyName` vs `StrategyName`), stale order.go comment, validate OK line missing counts | Fixed |
| LOW | Per-Pick allocations in pickers | Accepted for v1: measured negligible vs lock cost; revisit with profiler evidence |

Duplication candidates across limiter packages (state codec, checker boilerplate)
are recorded as refactor opportunities post-1.0 — current duplication is uniform,
conformance-covered, and not a defect.

## Design audit

Verdict: dashboard reads as a dense ops console, not a SaaS dashboard. Banned list:
clean (no emoji anywhere incl. logs/CLI, no gradients/glassmorphism/gauges/count-ups/
shadow-cards/default-indigo). Themes are pure token overrides; high-contrast raises
real contrast ratios; health is always dot+word.

Findings fixed:

| Sev | Finding | Disposition |
| --- | --- | --- |
| MED | Route-row STATUS column rendered hardcoded `-/-/-` | Fixed: real 2xx/4xx/5xx counts over trailing minute wired provider→DTO→DOM |
| MED | TLS version story contradicted itself across README/DESIGN/code | Fixed: single posture (tls12\|tls13 only) |
| MED | "Nesting depth ≤ 3" claim untrue for health/breaker blocks | Fixed spec restated honestly |
| MED | validate success line missing specified counts | Fixed: `OK (N routes, M pools, K limiters)` |
| MED | CLI error layout diverged from §3 sample | Fixed: `--> file:line:col` arrow lines |
| LOW | Human access-log shape drifted from sample | Fixed: DESIGN.md codifies shipped sorted-key form |
| LOW | JSON mode omits empty-string fields | Documented as contract |
| LOW | Chart window mismatch (spec 5 min/10 s vs shipped 60 s/1 s) | Spec updated: 60 s chart of trailing-five-minute percentiles |
| LOW | Inert `validate --no-color` flag | Removed |

## Post-audit verification

- `go build ./...`, `go vet ./...`: clean.
- `go test ./... -race -count=1`: all packages pass, including the limiter
  conformance suite over both drivers, the property tests, and the full-stack e2e
  suite (13 scenarios; strip-prefix/path-normalization/admin-host-guard verified
  through the real supervisor).
- Live smoke re-run: proxy end-to-end, token-bucket exhaustion with accurate
  RateLimit-*/Retry-After headers, hot reload applying burst change without dropped
  connections, admin auth enforced, trace events logged.

Zero open security or correctness findings remain at v1.0.0.
