// Package leakybucket implements the leaky_bucket limiting strategy.
//
// Each key tracks a queue level that rises by the cost of every admitted
// request and drains continuously at Limit/Window units per nanosecond, up
// to a hard Capacity. Output is smoothed: bursts pile onto the level and are
// released at the drain rate.
//
// The strategy is pure per DESIGN.md section 1: Step receives time from the
// caller, mutates only its decoded state copy, and never locks or reads a
// clock.
package leakybucket

import (
	"encoding/binary"
	"math"
	"time"

	"gatewright/internal/limiter"
)

// strategyName is the config-facing identifier this package registers under.
const strategyName = "leaky_bucket"

const (
	stateVersion = 1                // bump on incompatible state encodings
	stateLen     = 1 + 8 + 8        // version byte + float64 level + int64 last
	minRetry     = time.Millisecond // floor for computed retry hints
)

// Config is the validated knob set for one leaky_bucket instance.
type Config struct {
	Limit    int64         // drain rate: units removed per Window
	Window   time.Duration // drain period; >= 1ms
	Capacity int64         // maximum level; >= 1; 0 resolved to Limit upstream
}

// state is the persisted per-key queue level. Level is fractional so partial
// drainage is not lost between observations.
type state struct {
	Level float64
	Last  int64 // unix nanos of the previous observation
}

// algo is the pure strategy implementation wired into the engine by Factory.
type algo struct{ cfg Config }

func newAlgo(cfg Config) *algo { return &algo{cfg: cfg} }

// TTL bounds how long untouched key state survives. It exceeds the longest
// meaningful silence (a full bucket draining to empty) with margin; the
// one-minute floor keeps short windows from churning idle keys into fresh,
// empty buckets.
func (a *algo) TTL() time.Duration {
	drain := time.Duration(float64(a.cfg.Window) * float64(a.cfg.Capacity) / float64(a.cfg.Limit))
	ttl := 2*a.cfg.Window + 2*drain
	if ttl < time.Minute {
		ttl = time.Minute
	}
	return ttl
}

// rate is the drain speed in units per nanosecond.
func (a *algo) rate() float64 {
	return float64(a.cfg.Limit) / float64(a.cfg.Window.Nanoseconds())
}

// Step drains the bucket for the time elapsed since the stored observation,
// then tries to raise the level by cost.
//
// Decision.Limit carries Capacity: Remaining is measured against the bucket
// capacity, so Limit and Remaining stay on one scale. ResetIn is the time
// until the bucket is completely empty (zero when already empty). On denial
// RetryAfter is how long the caller must wait for enough drainage that
// level+cost would fit, floored at 1ms. Corrupt or absent state degrades to
// an empty bucket.
func (a *algo) Step(prev []byte, existed bool, now time.Time, cost int64) ([]byte, limiter.Decision) {
	st, ok := decode(prev)
	if !existed || !ok {
		st = state{Level: 0, Last: now.UnixNano()}
	}

	nowNanos := now.UnixNano()
	last := st.Last
	if last > nowNanos {
		last = nowNanos // backwards clock step: freeze rather than over-drain
	}
	level := st.Level - float64(nowNanos-last)*a.rate()
	if level < 0 {
		level = 0
	}

	allowed := cost >= 1 && level+float64(cost) <= float64(a.cfg.Capacity)
	if allowed {
		level += float64(cost)
	}
	d := limiter.Decision{
		Limit:   a.cfg.Capacity,
		ResetIn: time.Duration(level / a.rate()),
	}
	if allowed {
		d.Allowed = true
		d.Remaining = int64(math.Floor(float64(a.cfg.Capacity) - level))
	} else {
		d.Remaining = 0
		need := level + float64(cost) - float64(a.cfg.Capacity)
		if need < 0 {
			need = 0
		}
		d.RetryAfter = time.Duration(math.Ceil(need / a.rate()))
		if d.RetryAfter < minRetry {
			d.RetryAfter = minRetry
		}
	}
	return encode(state{Level: level, Last: nowNanos}), d
}

// encode serialises state: version byte first, then little-endian fields.
func encode(st state) []byte {
	buf := make([]byte, stateLen)
	buf[0] = stateVersion
	binary.LittleEndian.PutUint64(buf[1:], math.Float64bits(st.Level))
	binary.LittleEndian.PutUint64(buf[9:], uint64(st.Last))
	return buf
}

// decode parses a stored blob. Any malformation -- wrong length, unknown
// version, non-finite or negative level -- reports false so callers treat
// the key as absent and start from an empty bucket.
func decode(buf []byte) (state, bool) {
	if len(buf) != stateLen || buf[0] != stateVersion {
		return state{}, false
	}
	level := math.Float64frombits(binary.LittleEndian.Uint64(buf[1:]))
	last := int64(binary.LittleEndian.Uint64(buf[9:]))
	if math.IsNaN(level) || math.IsInf(level, 0) || level < 0 {
		return state{}, false
	}
	return state{Level: level, Last: last}, true
}
