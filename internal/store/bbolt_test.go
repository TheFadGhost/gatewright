package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"
)

// counterPayload encodes the test counter carried in the payload section
// (everything after the 8-byte expiry header).
func counterPayload(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}

func decodeCounter(t *testing.T, prev []byte, existed bool) uint64 {
	t.Helper()
	if !existed {
		return 0
	}
	if len(prev) != 8 {
		t.Fatalf("counter payload: got %d bytes, want 8", len(prev))
	}
	return binary.BigEndian.Uint64(prev)
}

// readCounter is the goroutine-safe variant used inside concurrent workers;
// corrupt payloads count as zero rather than touching t.
func readCounter(prev []byte) uint64 {
	if len(prev) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(prev)
}

func openTestDB(t *testing.T) *DB {
	t.Helper()
	d, err := Open(filepath.Join(t.TempDir(), "test.bolt"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// increment is the canonical read-modify-write through Backend.Update.
func increment(d *DB, key string) error {
	return d.Update(key, time.Hour, "counters", func(prev []byte, existed bool) []byte {
		_ = existed
		return counterPayload(readCounter(prev) + 1)
	})
}

// TestSetGetExpiry covers the basic lifecycle: absent -> live -> expired,
// with the expired record physically removed from the file.
func TestSetGetExpiry(t *testing.T) {
	d := openTestDB(t)

	err := d.Update("k", 10*time.Millisecond, "bkt", func(prev []byte, existed bool) []byte {
		if existed || prev != nil {
			t.Errorf("first update: existed=%v prev=%v, want fresh", existed, prev)
			return nil
		}
		return []byte("v1")
	})
	if err != nil {
		t.Fatalf("update 1: %v", err)
	}

	err = d.Update("k", 10*time.Millisecond, "bkt", func(prev []byte, existed bool) []byte {
		if !existed || string(prev) != "v1" {
			t.Errorf("second update: existed=%v prev=%q, want v1", existed, prev)
			return nil
		}
		return []byte("v2")
	})
	if err != nil {
		t.Fatalf("update 2: %v", err)
	}

	time.Sleep(15 * time.Millisecond) // only sleep in the suite; expiry assertion

	err = d.Update("k", time.Hour, "bkt", func(prev []byte, existed bool) []byte {
		if existed || prev != nil {
			t.Errorf("after expiry: existed=%v prev=%q, want absent", existed, prev)
			return nil
		}
		return nil // delete
	})
	if err != nil {
		t.Fatalf("update after expiry: %v", err)
	}

	// The expired-then-deleted key must be physically gone from the file.
	if err := d.db.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket([]byte("bkt"))
		if bt == nil {
			t.Fatal("bucket bkt missing")
		}
		if got := bt.Get([]byte("k")); got != nil {
			t.Errorf("key k still on disk: %q", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

// TestConcurrentSingleKeyNoLostUpdates is THE critical invariant: 64
// goroutines each perform 200 read-modify-write increments on ONE key. Every
// increment lands inside a serialized writer transaction, so the final count
// must be exactly 12800 -- any value below proves a lost update.
func TestConcurrentSingleKeyNoLostUpdates(t *testing.T) {
	const (
		goroutines = 64
		perG       = 200
		total      = goroutines * perG // 12800
	)
	d := openTestDB(t)

	errs := make(chan error, total)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if err := increment(d, "hot"); err != nil {
					errs <- fmt.Errorf("increment: %w", err)
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent increments failed: %v", err) // fail fast on first
	}

	got := uint64(0)
	err := d.Update("hot", time.Hour, "counters", func(prev []byte, existed bool) []byte {
		got = decodeCounter(t, prev, existed)
		return prev // no-op read
	})
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	if got != total {
		t.Fatalf("lost updates: final count = %d, want exactly %d", got, total)
	}
}

// TestCrossInstanceVisibility simulates cross-process sharing: instance A
// persists, its handle closes (releasing the file lock), instance B opens the
// SAME file and observes A's writes immediately -- then hands back so a fresh
// handle sees B's write in turn. Two handles cannot be held simultaneously by
// design: bbolt's exclusive writer file lock is what makes shared state safe.
func TestCrossInstanceVisibility(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.bolt")

	a, err := Open(path)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	if err := increment(a, "shared"); err != nil {
		t.Fatalf("A write: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("close A: %v", err)
	}

	b, err := Open(path)
	if err != nil {
		t.Fatalf("open B: %v", err)
	}
	var seen uint64
	err = b.Update("shared", time.Hour, "counters", func(prev []byte, existed bool) []byte {
		if !existed {
			t.Error("B cannot see A's write")
			return nil
		}
		seen = binary.BigEndian.Uint64(prev)
		return counterPayload(seen + 41)
	})
	if err != nil {
		t.Fatalf("B update: %v", err)
	}
	if seen != 1 {
		t.Fatalf("B read %d from A's write, want 1", seen)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close B: %v", err)
	}

	a2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen A: %v", err)
	}
	defer func() { _ = a2.Close() }()
	var final uint64
	err = a2.Update("shared", time.Hour, "counters", func(prev []byte, existed bool) []byte {
		final = binary.BigEndian.Uint64(prev)
		return prev
	})
	if err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if final != 42 {
		t.Fatalf("final value %d across instances, want 42", final)
	}
}

// TestCorruptForeignPayloadTolerated writes raw garbage straight into the
// bucket, bypassing the [header][payload] layout. Both flavors -- too short
// to carry a header, and a zeroed header (unix epoch, long expired) with a
// foreign tail -- must surface as the clean existed=false path, never panic,
// and be replaced by fresh state.
func TestCorruptForeignPayloadTolerated(t *testing.T) {
	d := openTestDB(t)

	garbage := [][]byte{
		[]byte("junk!"),  // 5 bytes: shorter than headerSize
		make([]byte, 20), // zeroed header => expired at 1970, plus tail
		append([]byte{0xff, 0xff}, bytes.Repeat([]byte{0x42}, 10)...), // alien nanos + body
	}
	for i, g := range garbage {
		key := fmt.Sprintf("corrupt-%d", i)
		if err := d.db.Update(func(tx *bbolt.Tx) error {
			bt, err := tx.CreateBucketIfNotExists([]byte("bkt"))
			if err != nil {
				return err
			}
			return bt.Put([]byte(key), g)
		}); err != nil {
			t.Fatalf("seed garbage %d: %v", i, err)
		}

		if err := d.Update(key, time.Hour, "bkt", func(prev []byte, existed bool) []byte {
			if existed || prev != nil {
				t.Errorf("garbage %d surfaced as live state: existed=%v prev=%q", i, existed, prev)
				return nil
			}
			return []byte("fresh")
		}); err != nil {
			t.Fatalf("update over garbage %d: %v", i, err)
		}

		// Garbage must be gone, replaced by a well-formed live record.
		if err := d.db.View(func(tx *bbolt.Tx) error {
			cur := tx.Bucket([]byte("bkt")).Get([]byte(key))
			if len(cur) <= headerSize || string(cur[headerSize:]) != "fresh" {
				t.Errorf("garbage %d not replaced cleanly: %q", i, cur)
			}
			return nil
		}); err != nil {
			t.Fatalf("view %d: %v", i, err)
		}
	}
}

// TestConcurrentDistinctKeysScale hammers many independent keys at once to
// prove the driver sustains parallel writers without deadlock or cross-key
// corruption. Each goroutine owns one key; totals are checked independently.
func TestConcurrentDistinctKeysScale(t *testing.T) {
	const (
		keys    = 32
		perKey  = 100
		timeout = 30 * time.Second // deadlock tripwire
	)
	d := openTestDB(t)

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for k := 0; k < keys; k++ {
			wg.Add(1)
			key := fmt.Sprintf("key-%02d", k)
			go func() {
				defer wg.Done()
				for i := 0; i < perKey; i++ {
					if err := increment(d, key); err != nil {
						t.Errorf("increment %s: %v", key, err)
						return
					}
				}
			}()
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("distinct-key concurrency deadlocked or starved")
	}

	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("key-%02d", k)
		want := uint64(perKey)
		err := d.Update(key, time.Hour, "counters", func(prev []byte, existed bool) []byte {
			if got := decodeCounter(t, prev, existed); got != want {
				t.Errorf("%s: count %d, want %d", key, got, want)
			}
			return prev
		})
		if err != nil {
			t.Fatalf("verify %s: %v", key, err)
		}
	}
}

// sweepWait polls cond until true or the deadline expires (sweeper timing).
func sweepWait(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", d, what)
}

// rawKeys lists every key physically present in a bucket, bypassing Update's
// lazy expiry handling — exactly what the sweeper must act on.
func rawKeys(t *testing.T, d *DB, bucket string) []string {
	t.Helper()
	var out []string
	if err := d.db.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket([]byte(bucket))
		if bt == nil {
			return nil
		}
		return bt.ForEach(func(k, _ []byte) error {
			out = append(out, string(k))
			return nil
		})
	}); err != nil {
		t.Fatalf("view %s: %v", bucket, err)
	}
	return out
}

func containsStr(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// TestSweeperDeletesExpiredAcrossBuckets seeds live and expired entries in
// two buckets, runs the sweeper at a tiny interval, and polls until ONLY the
// expired keys are physically gone from both buckets while the live one stays.
func TestSweeperDeletesExpiredAcrossBuckets(t *testing.T) {
	d := openTestDB(t)

	put := func(bucket, key string, ttl time.Duration) {
		t.Helper()
		if err := d.Update(key, ttl, bucket, func([]byte, bool) []byte {
			return []byte("v")
		}); err != nil {
			t.Fatalf("seed %s/%s: %v", bucket, key, err)
		}
	}
	put("bktA", "live-a", time.Hour)
	put("bktA", "dead-a", 15*time.Millisecond)
	put("bktB", "dead-b", 15*time.Millisecond)

	d.StartSweeper(context.Background(), 2*time.Millisecond)

	sweepWait(t, 5*time.Second, "sweeper to purge expired entries in both buckets", func() bool {
		a := rawKeys(t, d, "bktA")
		b := rawKeys(t, d, "bktB")
		return !containsStr(a, "dead-a") && !containsStr(b, "dead-b") &&
			containsStr(a, "live-a") && len(a) == 1 && len(b) == 0
	})

	a := rawKeys(t, d, "bktA")
	if !containsStr(a, "live-a") {
		t.Errorf("live entry swept too: bktA = %v", a)
	}
}

// TestSweeperStopsOnCloseAndContext proves both shutdown paths release the
// goroutine: Close stops it (and releases the file lock), and cancelling the
// context ends sweeping without touching the file.
func TestSweeperStopsOnCloseAndContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sweep.bolt")
	d, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.StartSweeper(ctx, time.Millisecond)
	cancel() // sweeper must wind down; nothing observable except no panic
	if err := d.Close(); err != nil {
		t.Fatalf("Close after ctx cancel: %v", err)
	}

	d2, err := Open(path) // lock must be free: proves the handle closed cleanly
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	d2.StartSweeper(context.Background(), time.Millisecond)
	if err := d2.Close(); err != nil {
		t.Fatalf("Close with active sweeper: %v", err)
	}
	// Handle is closed for business: updates fail loudly rather than hang.
	err = d2.Update("k", time.Hour, "b", func([]byte, bool) []byte { return nil })
	if err == nil {
		t.Error("Update after Close succeeded; expected closed-database error")
	}
}
