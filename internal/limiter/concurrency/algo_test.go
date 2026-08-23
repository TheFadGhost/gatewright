package concurrency

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"gatewright/internal/limiter"
)

var base = time.Unix(1_700_000_000, 0).UTC()

var cap5 = Config{Capacity: 5}

func TestAcquireReleaseCyclesRestoreFullAvailability(t *testing.T) {
	a := newAlgo(cap5)

	blob, d := a.Step(nil, false, base, 2)
	if !d.Allowed || d.Limit != 5 || d.Remaining != 3 {
		t.Fatalf("acquire 2: %+v", d)
	}
	if d.RetryAfter != 0 || d.ResetIn != 0 {
		t.Fatalf("admit must carry zero retry/reset: %+v", d)
	}

	blob, d = a.Step(blob, true, base, 3)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("acquire to capacity: %+v", d)
	}

	blob, d = a.Step(blob, true, base, 1)
	if d.Allowed || d.Remaining != 0 {
		t.Fatalf("saturated admit: %+v", d)
	}
	if d.RetryAfter != retryHint || d.ResetIn != 0 {
		t.Fatalf("denial must be fixed hint/zero reset: got %v/%v", d.RetryAfter, d.ResetIn)
	}
	if len(blob) != stateLen { // three units still held
		t.Fatalf("held state dropped: %d bytes", len(blob))
	}

	// The two completed requests (cost 2 and 3) release what they hold;
	// the denied attempt held nothing. Emptying the counter deletes state.
	rel := a.ReleaseStep(blob, true, base, 2)
	if rel == nil {
		t.Fatal("partial release deleted live holds")
	}
	if got := a.ReleaseStep(rel, true, base, 3); got != nil {
		t.Fatalf("emptied counter must delete state, got %d bytes", len(got))
	}

	// Fresh state: full availability again.
	blob, d = a.Step(nil, false, base, 5)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("full availability after release: %+v", d)
	}

	// Over-release clamps at zero rather than going negative.
	blob2, _ := a.Step(nil, false, base, 2)
	if got := a.ReleaseStep(blob2, true, base, 10); got != nil {
		t.Fatalf("over-release must clamp to deletion, got %d bytes", len(got))
	}
	if _, d = a.Step(nil, false, base, 5); !d.Allowed {
		t.Fatalf("clamped counter lost availability: %+v", d)
	}
}

func TestPartialReleaseKeepsRemainderHeld(t *testing.T) {
	a := newAlgo(cap5)
	blob, _ := a.Step(nil, false, base, 4)

	next := a.ReleaseStep(blob, true, base, 1)
	if next == nil {
		t.Fatal("partial release deleted live state")
	}
	st, ok := decode(next)
	if !ok || st.Current != 3 {
		t.Fatalf("after partial release: current=%d ok=%v, want 3", st.Current, ok)
	}
	// Exactly two slots remain available against capacity 5.
	_, d := a.Step(next, true, base, 2)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("remainder availability: %+v", d)
	}
	_, d = a.Step(next, true, base, 3)
	if d.Allowed || d.RetryAfter != retryHint {
		t.Fatalf("beyond remainder: %+v", d)
	}
}

func TestOverCapacityCostNeverAdmits(t *testing.T) {
	a := newAlgo(cap5)
	for i := 0; i < 3; i++ {
		blob, d := a.Step(nil, false, base, 6)
		if d.Allowed || d.Remaining != 0 || d.RetryAfter != retryHint || d.ResetIn != 0 {
			t.Fatalf("attempt %d: %+v", i, d)
		}
		if blob != nil {
			t.Fatal("unadmittable cost must not create state")
		}
	}
}

func TestNowIsIrrelevant(t *testing.T) {
	run := func(now time.Time) []limiter.Decision {
		a := newAlgo(cap5)
		var out []limiter.Decision
		blob, d := a.Step(nil, false, now, 3)
		out = append(out, d)
		blob, d = a.Step(blob, true, now.Add(time.Hour), 3) // deny
		out = append(out, d)
		release := a.ReleaseStep(blob, true, now.Add(2*time.Hour), 3)
		_, d = a.Step(release, release != nil, now.Add(3*time.Hour), 5)
		out = append(out, d)
		return out
	}
	early, late := run(base), run(base.Add(1000*time.Hour))
	for i := range early {
		if early[i] != late[i] {
			t.Fatalf("decision %d depends on now: %+v vs %+v", i, early[i], late[i])
		}
	}
}

func TestCorruptStateTreatedAsEmptyCounter(t *testing.T) {
	a := newAlgo(cap5)
	good, _ := a.Step(nil, false, base, 4)

	mutants := map[string][]byte{
		"truncated": good[:5],
		"extended":  append(append([]byte(nil), good...), 0x07),
		"version":   flipVersion(good),
		"negative":  withCurrent(good, math.MaxUint64), // int64 -1
		"garbage":   []byte("nope"),
		"nil":       nil,
	}
	for name, blob := range mutants {
		next, d := a.Step(blob, true, base, 5)
		if !d.Allowed || d.Remaining != 0 {
			t.Fatalf("%s: corrupt state must recover to empty counter, got %+v", name, d)
		}
		if len(next) != stateLen {
			t.Fatalf("%s: recovered blob is %d bytes", name, len(next))
		}
	}
}

func TestReleaseStepNoOps(t *testing.T) {
	a := newAlgo(cap5)
	if got := a.ReleaseStep(nil, false, base, 1); got != nil {
		t.Fatal("release on absent state must be a no-op")
	}
	if got := a.ReleaseStep([]byte("junk"), true, base, 1); got != nil {
		t.Fatal("release on corrupt state must be a no-op")
	}
	blob, _ := a.Step(nil, false, base, 2)
	if got := a.ReleaseStep(blob, true, base, 0); got == nil {
		t.Fatal("zero-cost release should leave live state untouched")
	}
	if got := a.ReleaseStep(blob, true, base, -2); got == nil {
		t.Fatal("negative-cost release should leave live state untouched")
	}
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	states := []state{{Current: 0}, {Current: 7}, {Current: 1 << 40}}
	for _, st := range states {
		got, ok := decode(encode(st))
		if !ok {
			t.Fatalf("decode(encode(%+v)) rejected", st)
		}
		if got != st {
			t.Fatalf("roundtrip mismatch: got %+v, want %+v", got, st)
		}
	}
	for _, bad := range [][]byte{nil, {}, {stateVersion}, make([]byte, stateLen+1)} {
		if _, ok := decode(bad); ok {
			t.Fatalf("decode accepted %d malformed bytes", len(bad))
		}
	}
}

func TestConfigFromDefaultsAndValidates(t *testing.T) {
	cfg, err := configFrom(limiter.Settings{Limit: 5}) // capacity defaults from limit
	if err != nil || cfg.Capacity != 5 {
		t.Fatalf("default capacity: cfg=%+v err=%v", cfg, err)
	}
	cfg, err = configFrom(limiter.Settings{Limit: 2, Capacity: 9})
	if err != nil || cfg.Capacity != 9 {
		t.Fatalf("explicit capacity wins: cfg=%+v err=%v", cfg, err)
	}
	for _, s := range []limiter.Settings{{}, {Limit: 0}, {Capacity: -1}} {
		if _, err := configFrom(s); err == nil {
			t.Fatalf("configFrom(%+v) accepted invalid settings", s)
		}
	}
}

func TestCheckerIgnoresWindow(t *testing.T) {
	if !limiter.Has(strategyName) {
		t.Fatal("strategy not registered")
	}
	for _, s := range []limiter.Settings{
		{Limit: 4, Window: 0},
		{Limit: 99, Window: time.Hour, Capacity: 2},
	} {
		if probs := limiter.CheckSettings(strategyName, s); len(probs) != 0 {
			t.Fatalf("CheckSettings(%+v) flagged valid settings: %v", s, probs)
		}
	}
	for _, s := range []limiter.Settings{{}, {Capacity: -3}} {
		if probs := limiter.CheckSettings(strategyName, s); len(probs) == 0 {
			t.Fatalf("CheckSettings(%+v) reported no problems", s)
		}
	}
}

func TestFactoryBuildsReleasableLimiter(t *testing.T) {
	l, err := limiter.New(strategyName, limiter.Params{Name: "cc", Settings: limiter.Settings{Limit: 3}})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if d := l.Allow("k", base, 2); !d.Allowed || d.Remaining != 1 {
		t.Fatalf("acquire through engine: %+v", d)
	}
	if d := l.Allow("k", base, 2); d.Allowed || d.RetryAfter != retryHint {
		t.Fatalf("saturated through engine: %+v", d)
	}
	rel, ok := limiter.AsReleaser(l)
	if !ok {
		t.Fatal("concurrency engine must expose Release")
	}
	rel.Release("k", base, 2)
	if d := l.Allow("k", base, 3); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("full availability after engine release: %+v", d)
	}
	if _, err := limiter.New(strategyName, limiter.Params{Name: "bad"}); err == nil {
		t.Fatal("factory accepted settings without capacity")
	}
	if ttl := newAlgo(cap5).TTL(); ttl <= 0 {
		t.Fatalf("TTL = %v, want > 0", ttl)
	}
}

func flipVersion(b []byte) []byte {
	out := append([]byte(nil), b...)
	if len(out) > 0 {
		out[0] ^= 0xFF
	}
	return out
}

func withCurrent(b []byte, bits uint64) []byte {
	out := append([]byte(nil), b...)
	if len(out) == stateLen {
		binary.LittleEndian.PutUint64(out[1:], bits)
	}
	return out
}
