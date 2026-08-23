package fixedwindow

import (
	"testing"
	"time"

	"gatewright/internal/limiter"
)

// base is aligned to whole 10s/1m boundaries so Truncate lands exactly on it.
var base = time.Unix(1_800_000_000, 0).UTC()

// TestBasicAdmitDenySequence walks one window from empty to full to denied.
func TestBasicAdmitDenySequence(t *testing.T) {
	a := algo{cfg: Config{Limit: 3, Window: 10 * time.Second}}

	next, d := a.Step(nil, false, base, 1)
	if !d.Allowed || d.Limit != 3 || d.Remaining != 2 || d.ResetIn != 10*time.Second {
		t.Fatalf("first admit: got %+v, want allowed rem=2 resetIn=10s", d)
	}
	next, d = a.Step(next, true, base.Add(time.Second), 1)
	if !d.Allowed || d.Remaining != 1 {
		t.Fatalf("second admit: got %+v, want allowed rem=1", d)
	}
	next, d = a.Step(next, true, base.Add(2*time.Second), 1)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("third admit: got %+v, want allowed rem=0", d)
	}
	_, d = a.Step(next, true, base.Add(3*time.Second), 1)
	if d.Allowed || d.Remaining != 0 {
		t.Fatalf("fourth call: got %+v, want denied rem=0", d)
	}
	if d.RetryAfter != 7*time.Second || d.ResetIn != 7*time.Second {
		t.Fatalf("deny timings: got retry=%v resetIn=%v, want 7s/7s", d.RetryAfter, d.ResetIn)
	}
}

// TestBoundaryRolloverExactlyAtEdge: the instant the window rolls over,
// quota is fresh again.
func TestBoundaryRolloverExactlyAtEdge(t *testing.T) {
	a := algo{cfg: Config{Limit: 1, Window: 10 * time.Second}}

	next, d := a.Step(nil, false, base.Add(4*time.Second), 1)
	if !d.Allowed || d.ResetIn != 6*time.Second {
		t.Fatalf("admit at +4s: got %+v, want allowed resetIn=6s", d)
	}
	_, d = a.Step(next, true, base.Add(9*time.Second+999999999), 1)
	if d.Allowed || d.RetryAfter != time.Nanosecond {
		t.Fatalf("deny at edge-1ns: got %+v, want denied retry=1ns", d)
	}
	_, d = a.Step(next, true, base.Add(10*time.Second), 1)
	if !d.Allowed || d.Remaining != 0 || d.ResetIn != 10*time.Second {
		t.Fatalf("admit at exact edge: got %+v, want allowed rem=0 resetIn=10s", d)
	}
}

// TestRemainingAccuracy checks Remaining tracks Limit-count exactly,
// including the zero clamp on denial with partial capacity left.
func TestRemainingAccuracy(t *testing.T) {
	a := algo{cfg: Config{Limit: 5, Window: 10 * time.Second}}

	var next []byte
	var d limiter.Decision
	for i, want := range []int64{4, 3, 2, 1, 0} {
		next, d = a.Step(next, i > 0, base.Add(time.Duration(i)*time.Second), 1)
		if !d.Allowed || d.Remaining != want {
			t.Fatalf("step %d: got %+v, want allowed rem=%d", i, d, want)
		}
	}
	_, d = a.Step(next, true, base.Add(5*time.Second), 3)
	if d.Allowed || d.Remaining != 0 {
		t.Fatalf("over-cost deny: got %+v, want denied rem=0", d)
	}
}

// TestRetryAfterPositiveOnDeny: RetryAfter counts down to the rollover and
// always stays positive.
func TestRetryAfterPositiveOnDeny(t *testing.T) {
	a := algo{cfg: Config{Limit: 1, Window: 10 * time.Second}}

	next, _ := a.Step(nil, false, base, 1)
	for _, at := range []time.Duration{0, time.Second, 7 * time.Second} {
		_, d := a.Step(next, true, base.Add(at), 1)
		if d.Allowed {
			t.Fatalf("at +%v: unexpectedly allowed", at)
		}
		if d.RetryAfter <= 0 || d.RetryAfter != 10*time.Second-at {
			t.Fatalf("at +%v: retry=%v, want %v", at, d.RetryAfter, 10*time.Second-at)
		}
	}
}

// TestCostAccounting: weighted requests debit exactly cost units.
func TestCostAccounting(t *testing.T) {
	a := algo{cfg: Config{Limit: 5, Window: 10 * time.Second}}

	next, d := a.Step(nil, false, base, 2)
	if !d.Allowed || d.Remaining != 3 {
		t.Fatalf("cost 2: got %+v, want allowed rem=3", d)
	}
	next, d = a.Step(next, true, base, 2)
	if !d.Allowed || d.Remaining != 1 {
		t.Fatalf("cost 2 again: got %+v, want allowed rem=1", d)
	}
	_, d = a.Step(next, true, base, 2)
	if d.Allowed || d.RetryAfter != 10*time.Second {
		t.Fatalf("cost 2 over: got %+v, want denied retry=10s", d)
	}
	_, d = a.Step(next, true, base, 1)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("cost 1 fits: got %+v, want allowed rem=0", d)
	}
}

// TestCorruptStateRecovery: malformed blobs are treated as absent, so the
// key starts from a full fresh window instead of failing.
func TestCorruptStateRecovery(t *testing.T) {
	a := algo{cfg: Config{Limit: 3, Window: 10 * time.Second}}

	overfull := append(encode(state{}), 0) // trailing garbage => wrong length
	corrupt := [][]byte{
		nil,
		{},
		{0xff},            // wrong version
		{stateVersion, 1}, // truncated
		overfull,
	}
	for _, c := range corrupt {
		_, d := a.Step(c, true, base, 3)
		if !d.Allowed || d.Remaining != 0 {
			t.Fatalf("corrupt % x: got %+v, want fresh-window allow of full limit", c, d)
		}
	}
}

// TestStateRoundTrip: encode->decode preserves fields; malformed input is
// rejected.
func TestStateRoundTrip(t *testing.T) {
	want := state{WindowStart: base.UnixNano(), Count: 42}
	got, ok := decode(encode(want))
	if !ok || got != want {
		t.Fatalf("roundtrip: got %+v ok=%v, want %+v", got, ok, want)
	}
	bad := [][]byte{
		nil,
		{0},
		{stateVersion},
		encode(want)[:len(encode(want))-1],
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
	valid := limiter.Settings{Limit: 10, Window: time.Second}
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
		{"both bad", limiter.Settings{}, 2},
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
		Name:     "fw-test",
		Settings: limiter.Settings{Limit: 2, Window: time.Minute},
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	d := l.Allow("k", base, 1)
	if !d.Allowed || d.Limit != 2 || d.Remaining != 1 || d.ResetIn != time.Minute {
		t.Fatalf("first allow: got %+v", d)
	}
	d = l.Allow("k", base.Add(time.Second), 1)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("second allow: got %+v", d)
	}
	d = l.Allow("k", base.Add(2*time.Second), 1)
	if d.Allowed || d.RetryAfter != time.Minute-2*time.Second {
		t.Fatalf("deny: got %+v", d)
	}
}
