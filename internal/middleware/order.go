// Package middleware defines the request pipeline. The ORDER below is a
// documented contract (DESIGN.md §6): it is enforced by Build in chain.go and
// surfaced by trace mode. Changing the order is a breaking change to
// Gatewright's observable behaviour.
package middleware

// Order is the fixed middleware execution sequence, outermost first.
//
//	 1. panic-recovery   — implicit outermost safety net; converts panics to INT500.
//	 2. request-id       — assigns/propagates X-Gatewright-Request-Id so every
//	                       later layer (and every log line) can be correlated.
//	 3. access-logging   — starts the timer, captures fields, emits after the
//	                       inner handler completes (including panics/errors).
//	 4. body-limit       — rejects oversized bodies BEFORE auth work is spent.
//	 5. total-timeout    — bounds the whole request lifetime via context deadline.
//	 6. CORS             — runs before auth so preflight OPTIONS never needs
//	                       credentials.
//	 7. auth             — API key / JWT verification populates identity used
//	                       by key extractors downstream.
//	 8. request-headers  — header manipulation on the inbound request.
//	 9. rate-limiting    — all configured limiters ANDed; the most restrictive
//	                       decision writes RateLimit-* / Retry-After headers.
//	10. routing/proxy    — forwarding with per-upstream connect/read/write
//	                       timeouts, retries (idempotent methods only,
//	                       jittered backoff) and the circuit breaker.
const (
	PosRequestID = iota
	PosAccessLog
	PosBodyLimit
	PosTotalTimeout
	PosCORS
	PosAuth
	PosRequestHeaders
	PosRateLimit
	PosProxy
	orderCount
)

// OrderNames names each position for trace output; index == position.
var OrderNames = [orderCount]string{
	"request-id",
	"access-log",
	"body-limit",
	"total-timeout",
	"cors",
	"auth",
	"request-headers",
	"rate-limit",
	"proxy",
}
