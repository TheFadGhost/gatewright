// Package concurrency implements the concurrency limiting strategy: a live
// counter of currently admitted units per key. Every admitted request holds
// its cost until released, so the middleware layer MUST drive Release on
// request completion; the engine surfaces it through limiter.Releaser.
//
// The strategy is pure per DESIGN.md section 1: no locks, no clock reads,
// state is a plain struct.
package concurrency

import (
	"encoding/binary"
	"time"

	"gatewright/internal/limiter"
)

// strategyName is the config-facing identifier this package registers under.
const strategyName = "concurrency"

const (
	// releaseTTL bounds how long untouched state survives. It exists to reap
	// slots leaked by crashed callers, not as part of the algorithm; it must
	// comfortably exceed any legitimate in-flight request (route timeouts are
	// minutes at worst).
	releaseTTL = time.Hour

	// retryHint is the fixed RetryAfter reported on denial. Actual
	// availability depends entirely on WHEN other holders call Release,
	// which cannot be predicted from stored state alone, so no meaningful
	// countdown can be computed; 100ms is a conservative polling hint, not a
	// deadline.
	retryHint = 100 * time.Millisecond

	stateVersion = 1     // bump on incompatible state encodings
	stateLen     = 1 + 8 // version byte + int64 current
)

// Config is the validated knob set for one concurrency instance.
type Config struct {
	Capacity int64 // maximum simultaneously held units; >= 1
}

// state is the persisted per-key live counter.
type state struct {
	Current int64
}

// algo is the pure strategy implementation wired into the engine by Factory.
type algo struct{ cfg Config }

func newAlgo(cfg Config) *algo { return &algo{cfg: cfg} }

// TTL bounds how long untouched key state survives (see releaseTTL).
func (a *algo) TTL() time.Duration { return releaseTTL }

// Step admits cost units while Current+cost fits within Capacity.
//
// There is no window: now is intentionally unused and ResetIn is zero,
// because the counter never resets on its own -- it only falls when units are
// released via ReleaseStep. Remaining reports Capacity-current-cost when
// admitted and zero otherwise. RetryAfter is always the fixed 100ms hint
// documented on retryHint; real availability depends on release timing.
// Corrupt or absent state degrades to an empty counter.
func (a *algo) Step(prev []byte, existed bool, _ time.Time, cost int64) ([]byte, limiter.Decision) {
	current := int64(0)
	if existed {
		if st, ok := decode(prev); ok {
			current = st.Current
		}
	}
	d := limiter.Decision{Limit: a.cfg.Capacity}
	if cost >= 1 && current+cost <= a.cfg.Capacity {
		current += cost
		d.Allowed = true
		d.Remaining = a.cfg.Capacity - current
	} else {
		d.Remaining = 0
		d.RetryAfter = retryHint
	}
	if !d.Allowed && current == 0 {
		return nil, d // nothing held (e.g. cost exceeds capacity): keep absent
	}
	return encode(state{Current: current}), d
}

// ReleaseStep hands back min(cost, Current) units previously admitted by
// Step. Releasing more than held clamps at an empty counter; an emptied
// counter returns nil so the key is dropped rather than kept alive as a zero
// entry. Absent or corrupt state is a no-op; a non-positive cost hands back
// nothing and preserves the existing holds untouched.
func (a *algo) ReleaseStep(prev []byte, existed bool, _ time.Time, cost int64) []byte {
	if !existed {
		return nil
	}
	st, ok := decode(prev)
	if !ok || st.Current <= 0 {
		return nil
	}
	if cost <= 0 {
		return encode(state{Current: st.Current})
	}
	remaining := st.Current - min(cost, st.Current)
	if remaining <= 0 {
		return nil
	}
	return encode(state{Current: remaining})
}

// encode serialises state: version byte first, then little-endian fields.
func encode(st state) []byte {
	buf := make([]byte, stateLen)
	buf[0] = stateVersion
	binary.LittleEndian.PutUint64(buf[1:], uint64(st.Current))
	return buf
}

// decode parses a stored blob. Any malformation -- wrong length, unknown
// version, negative count -- reports false so callers treat the key as
// absent (an empty counter).
func decode(buf []byte) (state, bool) {
	if len(buf) != stateLen || buf[0] != stateVersion {
		return state{}, false
	}
	current := int64(binary.LittleEndian.Uint64(buf[1:]))
	if current < 0 {
		return state{}, false
	}
	return state{Current: current}, true
}
