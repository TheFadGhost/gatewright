// Package tokenbucket implements the token_bucket limiting strategy.
//
// A bucket holds at most Burst tokens and refills continuously at
// Limit/Window tokens per nanosecond. Admission consumes whole tokens while
// fractional accumulation persists in state, so sustained throughput
// converges on exactly Limit per Window and instantaneous bursts are bounded
// by Burst.
//
// The strategy is pure per DESIGN.md section 1: Step receives time from the
// caller, mutates only its decoded state copy, and never locks or reads a
// clock.
package tokenbucket

import (
	"encoding/binary"
	"math"
	"time"

	"gatewright/internal/limiter"
)

// strategyName is the config-facing identifier this package registers under.
const strategyName = "token_bucket"

const (
	stateVersion = 1                // bump on incompatible state encodings
	stateLen     = 1 + 8 + 8        // version byte + float64 tokens + int64 last
	minRetry     = time.Millisecond // floor for computed retry hints
)

// Config is the validated knob set for one token_bucket instance.
type Config struct {
	Limit  int64         // tokens replenished per Window (sustained rate)
	Window time.Duration // refill period; >= 1ms
	Burst  int64         // bucket capacity; >= 1; 0 resolved to Limit upstream
}

// state is the persisted per-key bucket. Tokens are fractional so sub-token
// refills survive across calls instead of being silently discarded.
type state struct {
	Tokens float64
	Last   int64 // unix nanos of the previous observation
}

// algo is the pure strategy implementation wired into the engine by Factory.
type algo struct{ cfg Config }

func newAlgo(cfg Config) *algo { return &algo{cfg: cfg} }

// TTL bounds how long untouched key state survives. It exceeds the longest
// meaningful silence (an empty bucket refilling to full) with margin; the
// one-minute floor keeps short windows from churning idle keys into fresh,
// fully charged buckets.
func (a *algo) TTL() time.Duration {
	fill := time.Duration(float64(a.cfg.Window) * float64(a.cfg.Burst) / float64(a.cfg.Limit))
	ttl := 2*a.cfg.Window + 2*fill
	if ttl < time.Minute {
		ttl = time.Minute
	}
	return ttl
}

// rate is the refill speed in tokens per nanosecond.
func (a *algo) rate() float64 {
	return float64(a.cfg.Limit) / float64(a.cfg.Window.Nanoseconds())
}

// Step refills the bucket for the time elapsed since the stored observation,
// then tries to reserve cost tokens.
//
// Decision.Limit carries Burst: Remaining is measured against bucket
// capacity, so Limit and Remaining stay on one scale. ResetIn is the time to
// fill the bucket completely (zero when already full). On denial RetryAfter
// is the earliest instant at which cost tokens could have accumulated,
// floored at 1ms; a cost larger than Burst can never succeed and reports the
// fill-up horizon of an empty bucket. Corrupt or absent state degrades open:
// the key restarts with a full bucket.
func (a *algo) Step(prev []byte, existed bool, now time.Time, cost int64) ([]byte, limiter.Decision) {
	st, ok := decode(prev)
	if !existed || !ok {
		st = state{Tokens: float64(a.cfg.Burst), Last: now.UnixNano()}
	}

	nowNanos := now.UnixNano()
	last := st.Last
	if last > nowNanos {
		last = nowNanos // backwards clock step: freeze rather than refund
	}
	tokens := st.Tokens + float64(nowNanos-last)*a.rate()
	if burst := float64(a.cfg.Burst); tokens > burst {
		tokens = burst
	}

	allowed := cost >= 1 && float64(cost) <= tokens
	if allowed {
		tokens -= float64(cost)
	}
	d := limiter.Decision{
		Limit:   a.cfg.Burst,
		ResetIn: time.Duration((float64(a.cfg.Burst) - tokens) / a.rate()),
	}
	if allowed {
		d.Allowed = true
		d.Remaining = int64(math.Floor(tokens))
	} else {
		d.Remaining = 0
		need := float64(cost) - tokens
		if need < 0 {
			need = 0
		}
		d.RetryAfter = time.Duration(math.Ceil(need / a.rate()))
		if d.RetryAfter < minRetry {
			d.RetryAfter = minRetry
		}
	}
	if d.ResetIn < 0 {
		d.ResetIn = 0
	}
	return encode(state{Tokens: tokens, Last: nowNanos}), d
}

// encode serialises state: version byte first, then little-endian fields.
func encode(st state) []byte {
	buf := make([]byte, stateLen)
	buf[0] = stateVersion
	binary.LittleEndian.PutUint64(buf[1:], math.Float64bits(st.Tokens))
	binary.LittleEndian.PutUint64(buf[9:], uint64(st.Last))
	return buf
}

// decode parses a stored blob. Any malformation -- wrong length, unknown
// version, non-finite or negative token count -- reports false so callers
// treat the key as absent and start from a full bucket.
func decode(buf []byte) (state, bool) {
	if len(buf) != stateLen || buf[0] != stateVersion {
		return state{}, false
	}
	tokens := math.Float64frombits(binary.LittleEndian.Uint64(buf[1:]))
	last := int64(binary.LittleEndian.Uint64(buf[9:]))
	if math.IsNaN(tokens) || math.IsInf(tokens, 0) || tokens < 0 {
		return state{}, false
	}
	return state{Tokens: tokens, Last: last}, true
}
