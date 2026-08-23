package slidingcounter

import (
	"testing"
	"time"

	"gatewright/internal/limiter"
)

// base is aligned to whole seconds so cells (window 10s / 10 cells = 1s)
// land exactly on it; test instants use exact binary fractions (.25/.5/.75)
// so the interpolated weights are float-exact.
var base = time.Unix(1_800_000_000, 0).UTC()

// TestBasicAdmitDenySequence walks cells through admission and denial.
func TestBasicAdmitDenySequence(t *testing.T) {
	a := algo{cfg: Config{Limit: 10, Window: 10 * time.Second, Cells: 10}}

	next, d := a.Step(nil, false, base, 6)
	if !d.Allowed || d.Remaining != 4 || d.ResetIn != 10*time.Second {
		t.Fatalf("first admit: got %+v, want allowed rem=4 resetIn=10s", d)
	}
	// +500ms: same cell still; used=6, cost 4 fits exactly.
	next, d = a.Step(next, true, base.Add(500*time.Millisecond), 4)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("admit at +500ms: got %+v, want allowed rem=0", d)
	}
	// +1.5s: cell advanced (frac .5); prev cell weighs int64(10*.5)=5.
	next, d = a.Step(next, true, base.Add(time.Second+500*time.Millisecond), 5)
	if !d.Allowed || d.Remaining != 0 || d.ResetIn != 9500*time.Millisecond {
		t.Fatalf("admit at +1.5s: got %+v, want allowed rem=0 resetIn=9.5s", d)
	}
	// +1.75s: prev weighs int64(10*.25)=2, cur=5 => used=7; cost 4 overflows.
	_, d = a.Step(next, true, base.Add(time.Second+750*time.Millisecond), 4)
	if d.Allowed || d.Remaining != 3 || d.RetryAfter != 250*time.Millisecond {
		t.Fatalf("deny at +1.75s: got %+v, want denied rem=3 retry=250ms", d)
	}
}

// TestBoundaryRolloverExactlyAtEdge: at a cell boundary the previous cell
// still weighs fully (frac=0); half a cell later it halves; a multi-cell
// jump wipes state entirely (DESIGN.md §1 sample switch).
func TestBoundaryRolloverExactlyAtEdge(t *testing.T) {
	a := algo{cfg: Config{Limit: 10, Window: 10 * time.Second, Cells: 10}}

	next, _ := a.Step(nil, false, base, 10)

	// Exactly +1s: new cell, prev cell weighs 10*1.0=10 -> nothing fits.
	_, d := a.Step(next, true, base.Add(time.Second), 1)
	if d.Allowed || d.RetryAfter != time.Second {
		t.Fatalf("at exact cell edge: got %+v, want denied retry=1s", d)
	}
	// +1.5s: prev weighs 5 -> cost 5 fits.
	next, d = a.Step(next, true, base.Add(1500*time.Millisecond), 5)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("at +1.5s: got %+v, want allowed rem=0", d)
	}
	// +10s: jumped 9 cells ahead => long-gap fresh window, full capacity.
	_, d = a.Step(next, true, base.Add(10*time.Second), 10)
	if !d.Allowed || d.Remaining != 0 || d.ResetIn != 10*time.Second {
		t.Fatalf("after long gap: got %+v, want allowed rem=0 resetIn=10s", d)
	}
}

// TestCostAccounting: mixed weighted costs against interpolated usage,
// including the near-boundary truncation behaviour of the sample formula.
func TestCostAccounting(t *testing.T) {
	a := algo{cfg: Config{Limit: 10, Window: 10 * time.Second, Cells: 10}}

	next, d := a.Step(nil, false, base, 3)
	if !d.Allowed || d.Remaining != 7 {
		t.Fatalf("cost 3 fresh: got %+v, want allowed rem=7", d)
	}
	// +500ms: same cell; used=3; cost 8 overflows.
	next, d = a.Step(next, true, base.Add(500*time.Millisecond), 8)
	if d.Allowed || d.Remaining != 7 || d.RetryAfter != 500*time.Millisecond {
		t.Fatalf("cost 8 at +500ms: got %+v, want denied rem=7 retry=500ms", d)
	}
	// +1.25s: cell advanced (frac .25); prev weighs int64(3*.75)=2; cost 8 fits.
	next, d = a.Step(next, true, base.Add(time.Second+250*time.Millisecond), 8)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("cost 8 at +1.25s: got %+v, want allowed rem=0", d)
	}
	// +1.75s: prev weighs int64(3*.25)=0, cur=8; cost 3 denies; sample keeps
	// Remaining at the interpolated headroom (max(0, limit-used)).
	_, d = a.Step(next, true, base.Add(time.Second+750*time.Millisecond), 3)
	if d.Allowed || d.Remaining != 2 || d.RetryAfter != 250*time.Millisecond {
		t.Fatalf("cost 3 at +1.75s: got %+v, want denied rem=2 retry=250ms", d)
	}
	// +2.9s: next cell advanced; prev contributes int64(8*(1-.9))->0.
	_, d = a.Step(next, true, base.Add(2*time.Second+900*time.Millisecond), 2)
	if !d.Allowed || d.Remaining != 8 {
		t.Fatalf("cost 2 at +2.9s: got %+v, want allowed rem=8", d)
	}
}

// TestCorruptStateRecovery: malformed blobs are treated as absent.
func TestCorruptStateRecovery(t *testing.T) {
	a := algo{cfg: Config{Limit: 10, Window: 10 * time.Second, Cells: 10}}

	corrupt := [][]byte{
		nil,
		{},
		{stateVersion},
		{0xff},
		append(encode(state{}), 0),
	}
	for _, c := range corrupt {
		_, d := a.Step(c, true, base, 10)
		if !d.Allowed || d.Remaining != 0 {
			t.Fatalf("corrupt % x: got %+v, want fresh-state allow of full limit", c, d)
		}
	}
}

// TestStateRoundTrip: encode->decode preserves fields; malformed input is
// rejected.
func TestStateRoundTrip(t *testing.T) {
	want := state{Prev: 5, Cur: 7, CellIdx: 1_800_000_000}
	got, ok := decode(encode(want))
	if !ok || got != want {
		t.Fatalf("roundtrip: got %+v ok=%v, want %+v", got, ok, want)
	}
	bad := [][]byte{
		nil,
		{stateVersion},
		func() []byte { b := encode(want); return b[:len(b)-1] }(),
		func() []byte { b := encode(want); b[0] = 2; return b }(),
	}
	for _, c := range bad {
		if _, ok := decode(c); ok {
			t.Fatalf("decode(% x): unexpectedly ok", c)
		}
	}
}

// TestCheckerAndRegistration exercises the registered checker via
// limiter.CheckSettings and confirms factory registration.
func TestCheckerAndRegistration(t *testing.T) {
	if !limiter.Has(StrategyName) {
		t.Fatalf("%s not registered", StrategyName)
	}
	valid := limiter.Settings{Limit: 10, Window: time.Second} // Cells unset => default 10
	if probs := limiter.CheckSettings(StrategyName, valid); len(probs) != 0 {
		t.Fatalf("valid settings rejected: %v", probs)
	}
	cases := []struct {
		name string
		s    limiter.Settings
		want int
	}{
		{"no limit", limiter.Settings{Window: time.Second}, 1},
		{"no window", limiter.Settings{Limit: 1}, 1},
		{"cells too low", limiter.Settings{Limit: 1, Window: time.Second, Cells: 1}, 1},
		{"cells too high", limiter.Settings{Limit: 1, Window: time.Second, Cells: 1001}, 1},
		{"cells min ok", limiter.Settings{Limit: 1, Window: time.Second, Cells: 2}, 0},
		{"cells max ok", limiter.Settings{Limit: 1, Window: time.Second, Cells: 1000}, 0},
		{"all bad", limiter.Settings{Cells: 1}, 3},
	}
	for _, tc := range cases {
		if got := len(limiter.CheckSettings(StrategyName, tc.s)); got != tc.want {
			t.Fatalf("%s: got %d problems (%v), want %d", tc.name, got,
				limiter.CheckSettings(StrategyName, tc.s), tc.want)
		}
	}
}

// TestFactoryBuildsWorkingLimiter: Factory wires the engine end to end on the
// in-memory driver (key isolation is an engine concern, not retested here).
func TestFactoryBuildsWorkingLimiter(t *testing.T) {
	l, err := Factory(limiter.Params{
		Name:     "swc-test",
		Settings: limiter.Settings{Limit: 10, Window: 10 * time.Second},
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	d := l.Allow("k", base, 10)
	if !d.Allowed || d.Limit != 10 || d.Remaining != 0 {
		t.Fatalf("first allow: got %+v", d)
	}
	d = l.Allow("k", base.Add(500*time.Millisecond), 6)
	if d.Allowed || d.RetryAfter != 500*time.Millisecond {
		t.Fatalf("deny at +500ms: got %+v", d)
	}
	d = l.Allow("k", base.Add(time.Second+250*time.Millisecond), 3)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("allow at +1.25s: got %+v, want allowed rem=0", d)
	}
}
