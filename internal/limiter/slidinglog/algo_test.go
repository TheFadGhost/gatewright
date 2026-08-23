package slidinglog

import (
	"testing"
	"time"

	"gatewright/internal/limiter"
)

// base is aligned to whole 10s boundaries so expiry edges land exactly on it.
var base = time.Unix(1_800_000_000, 0).UTC()

// TestBasicAdmitDenySequence fills the window, denies, then verifies the
// RetryAfter points at the oldest event's expiry.
func TestBasicAdmitDenySequence(t *testing.T) {
	a := algo{cfg: Config{Limit: 3, Window: 10 * time.Second}}

	next, d := a.Step(nil, false, base, 1)
	if !d.Allowed || d.Limit != 3 || d.Remaining != 2 || d.ResetIn != 10*time.Second {
		t.Fatalf("first admit: got %+v, want allowed rem=2 resetIn=10s", d)
	}
	next, d = a.Step(next, true, base.Add(time.Second), 1)
	if !d.Allowed || d.Remaining != 1 || d.ResetIn != 9*time.Second {
		t.Fatalf("second admit: got %+v, want allowed rem=1 resetIn=9s", d)
	}
	next, d = a.Step(next, true, base.Add(2*time.Second), 1)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("third admit: got %+v, want allowed rem=0", d)
	}
	_, d = a.Step(next, true, base.Add(3*time.Second), 1)
	if d.Allowed || d.Remaining != 0 {
		t.Fatalf("fourth call: got %+v, want denied rem=0", d)
	}
	if d.RetryAfter != 7*time.Second { // oldest event (base) expires at base+10s
		t.Fatalf("deny retry: got %v, want 7s", d.RetryAfter)
	}
}

// TestSlidingExpiryAtExactEdge: events expire the instant now-window passes
// them, freeing capacity with no rollover boundary.
func TestSlidingExpiryAtExactEdge(t *testing.T) {
	a := algo{cfg: Config{Limit: 1, Window: 10 * time.Second}}

	next, _ := a.Step(nil, false, base.Add(5*time.Second), 1)
	_, d := a.Step(next, true, base.Add(15*time.Second).Add(-time.Nanosecond), 1)
	if d.Allowed || d.RetryAfter != time.Nanosecond {
		t.Fatalf("deny just before expiry: got %+v, want denied retry=1ns", d)
	}
	_, d = a.Step(next, true, base.Add(15*time.Second), 1)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("admit at exact expiry: got %+v, want allowed rem=0", d)
	}
}

// TestRemainingAndResetAccuracy: Remaining reflects stored units; ResetIn
// tracks the oldest surviving event.
func TestRemainingAndResetAccuracy(t *testing.T) {
	a := algo{cfg: Config{Limit: 2, Window: 10 * time.Second}}

	// Craft a state holding one event at base-5s; step admits one more now.
	s := state{events: []int64{base.Add(-5 * time.Second).UnixNano()}}
	next, d := a.Step(encode(s), true, base, 1)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("admit: got %+v, want allowed rem=0", d)
	}
	wantReset := 5 * time.Second // event at base-5s expires 5s from now
	if d.ResetIn != wantReset {
		t.Fatalf("resetIn: got %v, want %v", d.ResetIn, wantReset)
	}
	_, d = a.Step(next, true, base, 1)
	if d.Allowed || d.Remaining != 0 || d.RetryAfter <= 0 {
		t.Fatalf("deny: got %+v, want denied rem=0 retry>0", d)
	}
}

// TestCostAccounting: cost debits multiple units and denial RetryAfter
// accounts for exactly enough oldest events to free room.
func TestCostAccounting(t *testing.T) {
	a := algo{cfg: Config{Limit: 5, Window: 10 * time.Second}}

	next, d := a.Step(nil, false, base, 3)
	if !d.Allowed || d.Remaining != 2 || d.ResetIn != 10*time.Second {
		t.Fatalf("cost 3: got %+v, want allowed rem=2 resetIn=10s", d)
	}
	_, d = a.Step(next, true, base.Add(time.Second), 3)
	if d.Allowed || d.RetryAfter != 9*time.Second { // base event frees at +9s
		t.Fatalf("cost 3 over: got %+v, want denied retry=9s", d)
	}
	_, d = a.Step(next, true, base.Add(time.Second), 2)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("cost 2 fits: got %+v, want allowed rem=0", d)
	}
}

// TestCapStoredEventsAtLimit: an over-full (but well-formed) state is trimmed
// to the oldest limit events before deciding.
func TestCapStoredEventsAtLimit(t *testing.T) {
	a := algo{cfg: Config{Limit: 2, Window: 10 * time.Second}}

	s := state{events: []int64{
		base.Add(-8 * time.Second).UnixNano(),
		base.Add(-4 * time.Second).UnixNano(),
		base.Add(-1 * time.Second).UnixNano(),
	}}
	next, d := a.Step(encode(s), true, base, 1)
	if d.Allowed || d.RetryAfter != 6*time.Second { // base-4s event expires at +6s
		t.Fatalf("over-cap deny: got %+v, want denied retry=6s", d)
	}
	got, ok := decode(next)
	if !ok || len(got.events) != 2 || got.events[0] != s.events[1] || got.events[1] != s.events[2] {
		t.Fatalf("capped state: got %+v ok=%v, want oldest event dropped", got, ok)
	}
}

// TestCorruptStateRecovery: malformed blobs are treated as absent.
func TestCorruptStateRecovery(t *testing.T) {
	a := algo{cfg: Config{Limit: 3, Window: 10 * time.Second}}

	overfull := append(encode(state{}), 0) // length/count mismatch
	corrupt := [][]byte{
		nil,
		{},
		{stateVersion}, // truncated header
		{0xff},         // wrong version
		overfull,
	}
	for _, c := range corrupt {
		_, d := a.Step(c, true, base, 3)
		if !d.Allowed || d.Remaining != 0 {
			t.Fatalf("corrupt % x: got %+v, want fresh-window allow of full limit", c, d)
		}
	}
}

// TestStateRoundTrip: encode->decode preserves events; malformed input is
// rejected.
func TestStateRoundTrip(t *testing.T) {
	want := state{events: []int64{
		base.UnixNano(),
		base.Add(time.Second).UnixNano(),
		base.Add(3 * time.Second).UnixNano(),
	}}
	got, ok := decode(encode(want))
	if !ok || len(got.events) != len(want.events) {
		t.Fatalf("roundtrip: got %+v ok=%v", got, ok)
	}
	for i := range want.events {
		if got.events[i] != want.events[i] {
			t.Fatalf("event %d: got %d, want %d", i, got.events[i], want.events[i])
		}
	}
	bad := [][]byte{
		nil,
		{0},
		{stateVersion, 0, 0, 0},
		func() []byte { b := encode(want); b[1] = 9; return b }(), // count lies vs length
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
	if !limiter.Has(strategyName) {
		t.Fatalf("%s not registered", strategyName)
	}
	valid := limiter.Settings{Limit: 10, Window: time.Second}
	if probs := limiter.CheckSettings(strategyName, valid); len(probs) != 0 {
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
		if got := len(limiter.CheckSettings(strategyName, tc.s)); got != tc.want {
			t.Fatalf("%s: got %d problems (%v), want %d", tc.name, got,
				limiter.CheckSettings(strategyName, tc.s), tc.want)
		}
	}
}

// TestFactoryBuildsWorkingLimiter: Factory wires the engine end to end on the
// in-memory driver (key isolation is an engine concern, not retested here).
func TestFactoryBuildsWorkingLimiter(t *testing.T) {
	l, err := Factory(limiter.Params{
		Name:     "slog-test",
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
	d = l.Allow("k", base.Add(30*time.Second), 1)
	if d.Allowed || d.RetryAfter != 30*time.Second {
		t.Fatalf("deny: got %+v, want retry=30s", d)
	}
}
