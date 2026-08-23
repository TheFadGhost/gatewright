// Package slidingcounter implements the "sliding_window_counter" rate-
// limiting strategy: the window is divided into cells sub-windows and the
// previous cell's contribution is interpolated by the elapsed fraction
// (DESIGN.md §1). Approximate (±limit/cells typical); good memory middle
// ground. Semantics follow the reference sample in DESIGN.md §1 exactly.
//
// Step is pure: no locks, no clock reads beyond the supplied now, no globals.
package slidingcounter

import (
	"encoding/binary"
	"time"

	"gatewright/internal/limiter"
)

// DefaultCells is the sub-window count used when Settings.Cells is unset.
const DefaultCells = 10

// Config carries the validated knobs for this strategy.
type Config struct {
	Limit  int64         // max weighted units per window; >= 1
	Window time.Duration // window width; >= 1ms
	Cells  int64         // sub-windows; default 10, valid range 2..1000
}

// state is the persisted per-key state: only two cells are ever needed
// because the interpolated weight uses just the previous and current cell.
type state struct {
	Prev    int64 // units admitted in the previous cell
	Cur     int64 // units admitted in the current cell
	CellIdx int64 // absolute cell index of Cur
}

const (
	stateVersion = 1       // first byte of every encoded state
	stateSize    = 1 + 8*3 // version + prev + cur + cellIdx
)

// algo implements limiter.Strategy for sliding_window_counter.
type algo struct {
	cfg Config
}

// cellOf maps now to its absolute cell index and elapsed fraction within it.
func cellOf(now time.Time, w time.Duration, cells int64) (int64, float64) {
	cw := w / time.Duration(cells)
	abs := now.UnixNano() / int64(cw)
	frac := float64(now.UnixNano()%int64(cw)) / float64(int64(cw))
	return abs, frac
}

// Step accounts cost units against the key's state at instant now.
func (a algo) Step(prev []byte, existed bool, now time.Time, cost int64) ([]byte, limiter.Decision) {
	var s state
	if existed {
		s, _ = decode(prev) // corrupt state => treat as absent
	}

	abs, frac := cellOf(now, a.cfg.Window, a.cfg.Cells)
	switch {
	case abs == s.CellIdx+1: // advance one cell
		s.Prev, s.Cur = s.Cur, 0
		s.CellIdx = abs
	case abs > s.CellIdx+1: // long gap: fresh state
		s.Prev, s.Cur = 0, 0
		s.CellIdx = abs
	}

	used := int64(float64(s.Prev)*(1-frac)) + s.Cur
	cellWidth := a.cfg.Window / time.Duration(a.cfg.Cells)
	d := limiter.Decision{
		Limit:     a.cfg.Limit,
		Remaining: max(0, a.cfg.Limit-used),
		// full reset when the oldest contributing cell ages out completely
		ResetIn: time.Duration((1-frac)*float64(cellWidth)) + cellWidth*time.Duration(a.cfg.Cells-1),
	}
	if used+cost <= a.cfg.Limit {
		s.Cur += cost
		d.Allowed = true
		d.Remaining = max(0, a.cfg.Limit-(used+cost))
		return encode(s), d
	}
	// earliest retry: at the next cell boundary the previous cell's weight drops
	d.RetryAfter = max(time.Millisecond, time.Duration((1-frac)*float64(cellWidth)))
	return encode(s), d
}

// TTL bounds untouched state survival: two windows covers any live cells.
func (a algo) TTL() time.Duration { return 2 * a.cfg.Window }

// encode serialises state: version byte, then little-endian fields.
func encode(s state) []byte {
	b := make([]byte, stateSize)
	b[0] = stateVersion
	binary.LittleEndian.PutUint64(b[1:9], uint64(s.Prev))
	binary.LittleEndian.PutUint64(b[9:17], uint64(s.Cur))
	binary.LittleEndian.PutUint64(b[17:25], uint64(s.CellIdx))
	return b
}

// decode parses encoded state; anything malformed reports not-ok so callers
// can restart from a fresh state.
func decode(b []byte) (state, bool) {
	if len(b) != stateSize || b[0] != stateVersion {
		return state{}, false
	}
	return state{
		Prev:    int64(binary.LittleEndian.Uint64(b[1:9])),
		Cur:     int64(binary.LittleEndian.Uint64(b[9:17])),
		CellIdx: int64(binary.LittleEndian.Uint64(b[17:25])),
	}, true
}
