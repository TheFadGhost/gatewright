package leakybucket

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"gatewright/internal/limiter"
)

var base = time.Unix(1_700_000_000, 0).UTC()

// exactCfg drains at exactly 1 unit per nanosecond: all arithmetic below is
// free of floating point drift.
var exactCfg = Config{Limit: 1_000_000, Window: time.Millisecond, Capacity: 1_000_000}

// halfRateCfg drains at exactly 0.5 units per nanosecond.
var halfRateCfg = Config{Limit: 1_000_000, Window: 2 * time.Millisecond, Capacity: 1_000_000}

// smallCapCfg keeps a hard capacity below what the sustained rate could fill.
var smallCapCfg = Config{Limit: 1_000_000, Window: time.Millisecond, Capacity: 500_000}

// fill raises the level to capacity through sequential admits and returns
// the resulting state blob.
func fill(t *testing.T, a *algo, cfg Config, now time.Time) []byte {
	t.Helper()
	var blob []byte
	var d limiter.Decision
	for done := int64(0); done < cfg.Capacity; {
		blob, d = a.Step(blob, blob != nil, now, cfg.Capacity-done)
		if !d.Allowed {
			t.Fatalf("fill denied with %d remaining budget: %+v", cfg.Capacity-done, d)
		}
		done = cfg.Capacity - d.Remaining
	}
	return blob
}

func TestFreshBucketAdmitsAndReportsLevel(t *testing.T) {
	a := newAlgo(exactCfg)
	_, d := a.Step(nil, false, base, 400_000)
	if !d.Allowed || d.Limit != 1_000_000 || d.Remaining != 600_000 {
		t.Fatalf("fresh admit: allowed=%v limit=%d remaining=%d", d.Allowed, d.Limit, d.Remaining)
	}
	if d.ResetIn != 400*time.Microsecond { // level 400k draining at 1/ns
		t.Fatalf("ResetIn = %v, want 400us until empty", d.ResetIn)
	}
}

func TestDrainRunsBeforeEveryDecision(t *testing.T) {
	a := newAlgo(exactCfg)
	blob := fill(t, a, exactCfg, base) // level == capacity

	// Full bucket denies immediately.
	_, d := a.Step(blob, true, base, 1)
	if d.Allowed || d.Remaining != 0 || d.RetryAfter <= 0 {
		t.Fatalf("full bucket: %+v", d)
	}

	// Drainage happens inside the decision: +250us frees exactly 250k slots.
	blob, d = a.Step(blob, true, base.Add(250*time.Microsecond), 250_000)
	if !d.Allowed || d.Remaining != 0 { // refilled right up to capacity
		t.Fatalf("drain admit: allowed=%v remaining=%d", d.Allowed, d.Remaining)
	}
	_, d = a.Step(blob, true, base.Add(250*time.Microsecond), 1)
	if d.Allowed {
		t.Fatal("level must sit at capacity again after topping up")
	}

	// A full bucket left alone for one drain period empties completely.
	blob, d = a.Step(blob, true, base.Add(1250*time.Microsecond), 1_000_000)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("empty-bucket readmit: allowed=%v remaining=%d", d.Allowed, d.Remaining)
	}

	// ResetIn tracks continuous drainage mid-flight: a full bucket drains
	// 150k over 150us, leaving 850k queued => 850us until empty.
	blob = fill(t, a, exactCfg, base)
	blob, d = a.Step(blob, true, base.Add(150*time.Microsecond), 1_000_000) // overflow
	if d.Allowed {
		t.Fatal("1M on top of 850k must overflow")
	}
	if d.ResetIn != 850*time.Microsecond {
		t.Fatalf("ResetIn = %v, want 850us until empty", d.ResetIn)
	}
	if d.RetryAfter != time.Millisecond { // 150k-unit shortfall floors up
		t.Fatalf("RetryAfter = %v, want floored 1ms", d.RetryAfter)
	}
}

func TestCapacityBoundsBelowSustainedRate(t *testing.T) {
	a := newAlgo(smallCapCfg)
	blob, d := a.Step(nil, false, base, 500_000)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("capacity fill: %+v", d)
	}
	_, d = a.Step(blob, true, base, 1)
	if d.Allowed || d.RetryAfter <= 0 {
		t.Fatalf("over capacity: allowed=%v retry=%v", d.Allowed, d.RetryAfter)
	}
	// 1us later exactly 1k has drained: a 1k request fits, nothing more.
	blob, d = a.Step(blob, true, base.Add(time.Microsecond), 1_000)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("drain-sized admit: allowed=%v remaining=%d", d.Allowed, d.Remaining)
	}
	_, d = a.Step(blob, true, base.Add(time.Microsecond), 2)
	if d.Allowed {
		t.Fatal("level must remain pinned at capacity")
	}
}

func TestFractionalDrainPersistsAcrossCalls(t *testing.T) {
	a := newAlgo(halfRateCfg) // exactly 0.5 units per ns
	blob, d := a.Step(nil, false, base, 500_000)
	if !d.Allowed {
		t.Fatalf("initial admit: %+v", d)
	}
	// +1ns drains 0.5 => 499_999.5 held; a 500k request still fits.
	blob, d = a.Step(blob, true, base.Add(1), 500_000)
	if !d.Allowed || d.Remaining != 0 { // floor(capacity - 999_999.5) == 0
		t.Fatalf("fractional level admit: allowed=%v remaining=%d", d.Allowed, d.Remaining)
	}
	// The half-unit survived encoding: one more ns frees room for exactly 1.
	_, d = a.Step(blob, true, base.Add(2), 1)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("half-drained slot: %+v", d)
	}
}

func TestDenyRetryAfterAndFloors(t *testing.T) {
	a := newAlgo(exactCfg)
	blob := fill(t, a, exactCfg, base)

	_, d := a.Step(blob, true, base, 100_000)
	if d.Allowed || d.RetryAfter != time.Millisecond { // 100us shortfall floors up
		t.Fatalf("retry math: allowed=%v retry=%v, want false/1ms", d.Allowed, d.RetryAfter)
	}
	_, d = a.Step(blob, true, base, 1) // shortfall 1ns must still floor at 1ms
	if d.Allowed || d.RetryAfter != time.Millisecond {
		t.Fatalf("floor: allowed=%v retry=%v, want false/1ms", d.Allowed, d.RetryAfter)
	}
	// Cost far beyond capacity reports the full-drain horizon: 2M units of
	// backlog at exactly 1 unit/ns => 2ms, above the floor and thus exact.
	_, d = a.Step(blob, true, base, 2_000_000)
	if d.Allowed || d.RetryAfter != 2*time.Millisecond {
		t.Fatalf("oversized cost: allowed=%v retry=%v, want false/exactly 2ms", d.Allowed, d.RetryAfter)
	}
}

func TestCorruptStateRecoversEmpty(t *testing.T) {
	a := newAlgo(exactCfg)
	good, _ := a.Step(nil, false, base, 400_000)

	mutants := map[string][]byte{
		"truncated": good[:7],
		"extended":  append(append([]byte(nil), good...), 0x01),
		"version":   flipVersion(good),
		"nan":       withLevel(good, math.Float64bits(math.NaN())),
		"inf":       withLevel(good, math.Float64bits(math.Inf(-1))),
		"negative":  withLevel(good, math.Float64bits(-400_000)),
		"garbage":   []byte("not-a-leaky-bucket-state"),
		"nil":       nil,
	}
	for name, blob := range mutants {
		for _, existed := range []bool{true, false} {
			next, d := a.Step(blob, existed, base, 1)
			if !d.Allowed || d.Remaining != 999_999 {
				t.Fatalf("%s (existed=%v): corrupt state must recover to an empty bucket, got allowed=%v remaining=%d",
					name, existed, d.Allowed, d.Remaining)
			}
			if len(next) != stateLen {
				t.Fatalf("%s: recovered blob is %d bytes", name, len(next))
			}
		}
	}

	// Future timestamps are frozen, not treated as corruption.
	badTime := append([]byte(nil), good...)
	binary.LittleEndian.PutUint64(badTime[9:], uint64(math.MaxInt64))
	_, d := a.Step(badTime, true, base, 600_000) // level stays 400k, fits
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("future timestamp: allowed=%v remaining=%d", d.Allowed, d.Remaining)
	}
}

func TestBackwardsClockDoesNotOverDrain(t *testing.T) {
	a := newAlgo(exactCfg)
	blob, _ := a.Step(nil, false, base, 400_000) // level 400k

	_, d := a.Step(blob, true, base.Add(-time.Microsecond), 700_000)
	if d.Allowed {
		t.Fatal("a backwards clock step must not drain extra units")
	}
	if d.RetryAfter < minRetry {
		t.Fatalf("denial RetryAfter = %v, want >= 1ms", d.RetryAfter)
	}
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	states := []state{
		{Level: 999_999.5, Last: 987654321},
		{Level: 0, Last: 0},
		{Level: 12345.678901234, Last: -5},
	}
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

func TestConfigFromValidatesAndDefaults(t *testing.T) {
	cfg, err := configFrom(limiter.Settings{Limit: 50, Window: time.Second})
	if err != nil || cfg.Capacity != 50 {
		t.Fatalf("capacity default: cfg=%+v err=%v", cfg, err)
	}
	cfg, err = configFrom(limiter.Settings{Limit: 100, Window: time.Second, Capacity: 10})
	if err != nil || cfg.Capacity != 10 {
		t.Fatalf("explicit capacity: cfg=%+v err=%v", cfg, err)
	}
	for _, s := range []limiter.Settings{
		{},
		{Limit: 0, Window: time.Second},
		{Limit: 5, Window: 0},
		{Limit: 5, Window: time.Millisecond - 1},
		{Limit: 5, Window: time.Second, Capacity: -1},
	} {
		if _, err := configFrom(s); err == nil {
			t.Fatalf("configFrom(%+v) accepted invalid settings", s)
		}
	}
}

func TestCheckerRegistered(t *testing.T) {
	if !limiter.Has(strategyName) {
		t.Fatal("strategy not registered")
	}
	if probs := limiter.CheckSettings(strategyName, limiter.Settings{Limit: 10, Window: time.Second}); len(probs) != 0 {
		t.Fatalf("valid settings flagged: %v", probs)
	}
	for _, s := range []limiter.Settings{
		{},
		{Limit: 0, Window: time.Second},
		{Limit: 10, Window: 0},
		{Limit: 10, Window: time.Second, Capacity: -2},
	} {
		if probs := limiter.CheckSettings(strategyName, s); len(probs) == 0 {
			t.Fatalf("CheckSettings(%+v) reported no problems", s)
		}
	}
}

func TestFactoryBuildsWorkingLimiter(t *testing.T) {
	l, err := limiter.New(strategyName, limiter.Params{
		Name:     "lb",
		Settings: limiter.Settings{Limit: 100, Window: time.Second}, // capacity defaults to 100
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if d := l.Allow("k", base, 60); !d.Allowed || d.Remaining != 40 {
		t.Fatalf("first allow: %+v", d)
	}
	if d := l.Allow("k", base, 60); d.Allowed || d.RetryAfter <= 0 || d.ResetIn <= 0 {
		t.Fatalf("overflow through engine: %+v", d)
	}
	if d := l.Allow("other", base, 60); !d.Allowed {
		t.Fatalf("keys must be independent: %+v", d)
	}
	if _, err := limiter.New(strategyName, limiter.Params{Name: "bad"}); err == nil {
		t.Fatal("factory accepted empty settings")
	}
	if ttl := newAlgo(exactCfg).TTL(); ttl <= 0 {
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

func withLevel(b []byte, bits uint64) []byte {
	out := append([]byte(nil), b...)
	if len(out) == stateLen {
		binary.LittleEndian.PutUint64(out[1:], bits)
	}
	return out
}
