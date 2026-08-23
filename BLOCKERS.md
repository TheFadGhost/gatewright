# BLOCKERS.md

None open.

## Resolved

- 2026-08-23 — Trace-header limitation: the `X-Gatewright-Trace` response header can only carry stage timings that completed before the upstream responded, because header emission is bound to the response. Decision: the response header reports the matched route plus pre-response stages only; the complete pipeline (every stage, inclusive durations, completion order) is emitted as a structured `request-trace` log event with `req_id`, `route`, `status`, and `stages`. Documented in DESIGN.md §6 and implemented by the trace gate in internal/runtime/assemble.go.
