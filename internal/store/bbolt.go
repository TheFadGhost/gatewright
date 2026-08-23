// Package store hosts the shared-store limiter backend.
//
// Atomicity model: bbolt permits exactly ONE writer at a time, enforced by an
// exclusive OS-level file lock on the database file. Every Update below is a
// single bbolt.Update (read-modify-write) transaction, so two gateway
// processes sharing one file can never interleave: whichever process holds
// the file lock finishes its whole transaction before the other can even open
// for writing. Lost updates are therefore impossible without any extra
// locking here -- correctness is delegated entirely to bbolt's writer
// serialization, exactly as DESIGN.md section 1 prescribes.
package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"time"

	bbolt "go.etcd.io/bbolt"

	"gatewright/internal/limiter"
)

// headerSize is the byte width of the big-endian absolute-expiry timestamp
// prefixed to every stored value.
const headerSize = 8

// metaBucket is created at Open so the database always has a well-defined
// root bucket even before the first limiter write lands.
var metaBucket = []byte("meta")

// DB is the bbolt-backed limiter.Backend. It is safe for concurrent use by
// multiple goroutines and, thanks to the file lock, across processes.
type DB struct {
	db *bbolt.DB

	sweepOnce sync.Once
	sweepStop chan struct{}
	closeOnce sync.Once
}

// Compile-time proof that the production driver satisfies the audited
// Backend contract the conformance suite exercises.
var _ limiter.Backend = (*DB)(nil)

// Open opens (creating the file if missing) the bbolt database at path.
// It waits up to 1s to acquire the writer file lock -- long enough to absorb
// a sibling process mid-transaction, short enough to fail loudly rather than
// hang a gateway startup.
func Open(path string) (*DB, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	d := &DB{db: db, sweepStop: make(chan struct{})}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(metaBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return d, nil
}

// Update applies fn to the state blob under key inside ONE bbolt writer
// transaction. Stored layout is [8-byte big-endian expireUnixNano][payload].
//
// An entry whose header timestamps at or before now is expired: the stale
// bytes are deleted, fn observes existed=false with a nil prev, and whatever
// fn returns is stored as a brand-new record with expiry now+ttl. Corrupt or
// foreign payloads (shorter than the header, or an unparseable/expired
// header) take the same absent path -- they never panic and never leak into
// fn as live state.
//
// Returning nil from fn deletes the key. Non-nil next is persisted with a
// refreshed expiry (sliding TTL), mirroring the in-memory driver.
//
// Because bbolt serializes all writers via the file lock (intra-process and
// cross-process alike), the read inside this transaction cannot race any
// other write: read-modify-write is atomic end to end.
func (d *DB) Update(key string, ttl time.Duration, bucket string,
	fn func(prev []byte, existed bool) (next []byte)) error {

	name := sanitizeBucket(bucket)
	nowNanos := time.Now().UnixNano()
	expireNanos := nowNanos + int64(ttl)

	return d.db.Update(func(tx *bbolt.Tx) error {
		bt, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return err
		}
		k := []byte(key)

		var prev []byte
		existed := false
		cur := bt.Get(k)
		if len(cur) >= headerSize {
			if expire := int64(binary.BigEndian.Uint64(cur[:headerSize])); expire > nowNanos {
				prev = bytes.Clone(cur[headerSize:])
				existed = true
			}
		}
		if !existed && len(cur) > 0 {
			// Expired or corrupt: purge so the fresh record below starts clean.
			if err := bt.Delete(k); err != nil {
				return err
			}
		}

		next := fn(prev, existed)
		if next == nil {
			return bt.Delete(k)
		}
		buf := make([]byte, headerSize+len(next))
		binary.BigEndian.PutUint64(buf[:headerSize], uint64(expireNanos))
		copy(buf[headerSize:], next)
		return bt.Put(k, buf)
	})
}

// StartSweeper launches a background goroutine that periodically deletes
// every expired entry across all buckets, one writer transaction per bucket,
// so dead state cannot accumulate between touches of a key. Safe to call at
// most once per DB; further calls are no-ops. A non-positive interval starts
// nothing. Close stops the sweeper before closing the file.
func (d *DB) StartSweeper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	d.sweepOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-d.sweepStop:
					return
				case <-ticker.C:
					d.sweepExpired()
				}
			}
		}()
	})
}

// sweepExpired purges expired records bucket by bucket. Keys are collected
// under the cursor first and deleted afterwards: bbolt cursors see a stable
// view, and mutation mid-iteration is undefined for the page being walked.
func (d *DB) sweepExpired() {
	var buckets [][]byte
	if err := d.db.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			buckets = append(buckets, bytes.Clone(name))
			return nil
		})
	}); err != nil {
		return // closed or unreadable: nothing to sweep
	}
	now := time.Now().UnixNano()
	for _, name := range buckets {
		_ = d.db.Update(func(tx *bbolt.Tx) error {
			bt := tx.Bucket(name)
			if bt == nil {
				return nil
			}
			var dead [][]byte
			c := bt.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if len(v) >= headerSize &&
					int64(binary.BigEndian.Uint64(v[:headerSize])) <= now {
					dead = append(dead, bytes.Clone(k))
				}
			}
			for _, k := range dead {
				if err := bt.Delete(k); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

// Close releases the file lock and flushes pending writes. It also stops a
// sweeper started by StartSweeper.
func (d *DB) Close() error {
	d.closeOnce.Do(func() { close(d.sweepStop) })
	return d.db.Close()
}

// sanitizeBucket maps a logical bucket id onto a legal bbolt bucket name by
// replacing every rune outside [A-Za-z0-9_.-] with '_'. The engine builds
// buckets as "<route>/<name>", so '/' becomes '_' deterministically.
func sanitizeBucket(bucket string) string {
	b := []byte(bucket)
	for i, c := range b {
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z',
			c >= '0' && c <= '9', c == '_', c == '.', c == '-':
		default:
			b[i] = '_'
		}
	}
	if len(b) == 0 {
		return "default" // bbolt rejects empty bucket names
	}
	return string(b)
}
