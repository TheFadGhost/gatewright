// Package slidinglog implements the "sliding_window_log" rate-limiting
// strategy: exact admission over the trailing window by storing one timestamp
// per admitted unit (DESIGN.md §1). Most accurate; heaviest per key. Stored
// events are capped at Limit so memory stays bounded.
//
// Step is pure: no locks, no clock reads beyond the supplied now, no globals.
package slidinglog

import (
	"encoding/binary"
	"slices"
	"time"

	"gatewright/internal/limiter"
)

// Config carries the validated knobs for this strategy.
type Config struct {
	Limit  int64         // max units admitted per window; >= 1
	Window time.Duration // window width; >= 1ms
}

// state is the persisted per-key state: timestamps (unix nanos) of every
// admitted unit still inside the window, ascending.
type state struct {
	events []int64
}

const stateVersion = 1 // first byte of every encoded state

// algo implements limiter.Strategy for sliding_window_log.
type algo struct {
	cfg Config
}

// Step accounts cost units against the key's state at instant now.
func (a algo) Step(prev []byte, existed bool, now time.Time, cost int64) ([]byte, limiter.Decision) {
	var s state
	if existed {
		s, _ = decode(prev) // corrupt state => treat as absent
	}
	nowN := now.UnixNano()
	windowStartN := nowN - int64(a.cfg.Window)

	s.prune(windowStartN)
	s.capTo(a.cfg.Limit)

	allowed := int64(len(s.events))+cost <= a.cfg.Limit
	if allowed {
		for i := int64(0); i < cost; i++ {
			s.events = append(s.events, nowN)
		}
		slices.Sort(s.events) // tolerate non-monotonic now between calls
	}

	d := limiter.Decision{Limit: a.cfg.Limit}
	if allowed {
		d.Allowed = true
		d.Remaining = a.cfg.Limit - int64(len(s.events))
	} else {
		d.Remaining = 0
		d.RetryAfter = retryAfter(a.cfg.Limit, a.cfg.Window, windowStartN, s.events, cost)
	}
	d.ResetIn = resetIn(windowStartN, s.events)
	return encode(s), d
}

// TTL bounds untouched state survival: two windows covers any live event.
func (a algo) TTL() time.Duration { return 2 * a.cfg.Window }

// prune drops events at or before the window start; an event expires once
// now-t >= window. Input must be ascending (decode enforces this).
func (s *state) prune(windowStartN int64) {
	i := 0
	for i < len(s.events) && s.events[i] <= windowStartN {
		i++
	}
	s.events = s.events[i:]
}

// capTo keeps at most limit events, dropping the oldest; bounds memory even
// for over-full states written by hand or by other versions.
func (s *state) capTo(limit int64) {
	if n := int(limit); n >= 0 && len(s.events) > n {
		s.events = s.events[len(s.events)-n:]
	}
}

// retryAfter reports when enough of the oldest events expire for cost to fit:
// after k = used+cost-limit of the oldest events age out, exactly cost slots
// remain, so the k-th oldest event's expiry is the earliest useful retry.
func retryAfter(limit int64, window time.Duration, windowStartN int64, events []int64, cost int64) time.Duration {
	if cost > limit || len(events) == 0 { // nothing within this window can ever fit
		return window
	}
	k := int64(len(events)) + cost - limit
	if k >= 1 && k <= int64(len(events)) {
		return time.Duration(events[k-1] - windowStartN)
	}
	return window
}

// resetIn reports when every stored event has aged out (full refill); zero
// when none are stored.
func resetIn(windowStartN int64, events []int64) time.Duration {
	if len(events) == 0 {
		return 0
	}
	return time.Duration(events[0] - windowStartN)
}

// encode serialises state: version byte, uint32 event count, then each event
// as a little-endian int64.
func encode(s state) []byte {
	b := make([]byte, 5+8*len(s.events))
	b[0] = stateVersion
	binary.LittleEndian.PutUint32(b[1:5], uint32(len(s.events)))
	for i, e := range s.events {
		binary.LittleEndian.PutUint64(b[5+8*i:], uint64(e))
	}
	return b
}

// decode parses encoded state; anything malformed reports not-ok so callers
// can restart from a fresh state.
func decode(b []byte) (state, bool) {
	if len(b) < 5 || b[0] != stateVersion {
		return state{}, false
	}
	n := int(binary.LittleEndian.Uint32(b[1:5]))
	if len(b) != 5+8*n {
		return state{}, false
	}
	events := make([]int64, n)
	for i := range events {
		events[i] = int64(binary.LittleEndian.Uint64(b[5+8*i:]))
	}
	slices.Sort(events) // canonical ascending order regardless of writer
	return state{events: events}, true
}
