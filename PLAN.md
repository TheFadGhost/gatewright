# PLAN.md — feature ideation record

Every proposed item was judged against three tests:
1. Does it serve the core purpose — proxying with pluggable rate limiting?
2. Can it be finished to the same quality bar as the rest?
3. Does it avoid expanding Gatewright into a second product?

## Accepted

| Idea | One-line reason |
| --- | --- |
| Dry-run / config-diff mode (`gatewright validate --diff old.yaml`) | Operators change configs at 3am; seeing exactly what changed before reload is cheap and prevents outages. |
| Safe-by-default posture (deny unmatched routes, bounded body limits, bounded limiter key space, TLS verification on) | An operations tool must fail closed; permissive defaults are how incidents happen. |
| Errors naming the exact config path (`routes[2].rate_limits[0].window`) with file:line | Cuts mean-time-to-fix for config mistakes; direct extension of the validation story. |
| Request-trace mode (`X-Gatewright-Trace: 1` returns matched route + middleware timings) | Makes the documented middleware chain observable; tiny cost, high reasoning value. |
| Per-route overrides (auth/CORS/limits/timeouts attach per route, not globally) | Already the natural shape of the route model; making it explicit costs nothing extra. |
| Shadow/mirror traffic to a second upstream pool (fire-and-forget) | Directly serves testing new upstreams through the real proxy path; small, bounded addition to the forwarder. |
| Load-test harness in-repo (`test/load`) | The project's own claims require a repeatable load generator; also serves operators. |
| Keyboard navigation in the dashboard | Ops tools are used mid-incident from keyboards; accessibility is part of the quality bar anyway. |
| Structured error responses with a consistent JSON shape | One envelope (`{error:{code,message,req_id}}`) across proxy, admin API, and logs removes guesswork. |
| Shared-store limiter backend (bbolt-backed) alongside in-memory | Proves the limiter contract holds beyond a single process; correctness owned by the storage driver, tested identically. |

## Rejected

| Idea | One-line reason |
| --- | --- |
| Service mesh / sidecar injection | Second product; different deployment model, different failure domain. |
| Full API management portal (developer portal, billing, API keys CRUD UI) | Second product; Gatewright reads keys from config/env, it does not manage customer accounts. |
| WAF with a rule marketplace | Second product; rule authoring ecosystems are their own quality bar. |
| Service discovery integrations (Consul/K8s/DNS watchers) | Second product; static targets + health checking cover the core, discovery drags in cluster assumptions. |
| Plugin scripting language (embedded Lua/WASM hooks) | Second product; the Go plugin surface (limiters, middleware interfaces) already covers extension points safely. |
| Distributed tracing export (OTLP collector pipeline) | Observability scope creep; structured logs + Prometheus + trace mode answer "what handled this request" without a collector dependency. |
| Multi-gateway clustering / config sync between instances | Second product; the shared-store backend solves the actual cross-process problem (shared limiter state) without a control plane. |
| Response caching layer | Different correctness domain (invalidation); competes with dedicated caches and doubles the test matrix for little core value. |
| GraphQL/gRPC protocol-aware routing | Protocol specialization beyond HTTP reverse proxying; each protocol brings its own ecosystem. |
| Theme gallery for the dashboard | An ops tool needs three readable themes (dark, light, high-contrast), not decoration options. |
