// Package fixedwindow implements the "fixed_window" rate-limiting strategy:
// a per-key counter that resets at each window boundary truncated from now
// (DESIGN.md §1). Cheapest strategy; permits up to 2x limit across a boundary
// burst by design.
//
// Step is pure: state arrives as an opaque blob and leaves as one, time is
// supplied and never read, and no locks or globals exist here. All concurrency
// belongs to the engine drivers.
package fixedwindow

import (
	"encoding/binary"
	"time"

	"gatewright/internal/limiter"
)

// Config carries the validated knobs for this strategy.
type Config struct {
	Limit  int64         // max units admitted per window; >= 1
	Window time.Duration // window width; >= 1ms
}

// state is the persisted per-key state.
type state struct {
	WindowStart int64 // unix nanos of the current window's start
	Count       int64 // units admitted within the current window
}

const (
	stateVersion = 1           // first byte of every encoded state
	stateSize    = 1 + 8 + 8   // version + windowStart + count
)

// algo implements limiter.Strategy for fixed_window.
type algo struct {
	cfg Config
}

// Step accounts cost units against the key's state at instant now.
func (a algo) Step(prev []byte, existed bool, now time.Time, cost int64) ([]byte, limiter.Decision) {
	var s state
	if existed {
		s, _ = decode(prev) // corrupt state => treat as absent
	}

	start := now.Truncate(a.cfg.Window).UnixNano()
	if start != s.WindowStart { // first sight or window rolled over
		s.WindowStart, s.Count = start, 0
	}

	d := limiter.Decision{
		Limit:     a.cfg.Limit,
		ResetIn:   a.cfg.Window - now.Sub(time.Unix(0, start)),
		Remaining: max(0, a.cfg.Limit-s.Count),
	}
	if s.Count+cost <= a.cfg.Limit {
		s.Count += cost
		d.Allowed = true
		d.Remaining = a.cfg.Limit - s.Count
		return encode(s), d
	}
	d.Remaining = 0
	d.RetryAfter = d.ResetIn // first slot frees at the window rollover
	return encode(s), d
}

// TTL bounds untouched state survival: two windows covers any live count.
func (a algo) TTL() time.Duration { return 2 * a.cfg.Window }

// encode serialises state: version byte, then little-endian fields.
func encode(s state) []byte {
	b := make([]byte, stateSize)
	b[0] = stateVersion
	binary.LittleEndian.PutUint64(b[1:9], uint64(s.WindowStart))
	binary.LittleEndian.PutUint64(b[9:17], uint64(s.Count))
	return b
}

// decode parses encoded state; anything malformed reports not-ok so callers
// can restart from a fresh state.
func decode(b []byte) (state, bool) {
	if len(b) != stateSize || b[0] != stateVersion {
		return state{}, false
	}
	return state{
		WindowStart: int64(binary.LittleEndian.Uint64(b[1:9])),
		Count:       int64(binary.LittleEndian.Uint64(b[9:17])),
	}, true
}
