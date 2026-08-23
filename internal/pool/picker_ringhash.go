package pool

import (
	"sort"
	"strconv"
	"sync"
	"time"
)

const vnodeFactor = 160

type vnode struct {
	hash uint32
	idx  int
}

// ringHashPicker implements ketama-style consistent hashing: each target
// contributes 160 virtual nodes per weight unit placed by FNV-1a of
// "<name>#<i>", lookups hash the key with FNV-1a and walk the ring clockwise,
// skipping ineligible targets. An empty key falls back to deterministic
// rotation across targets.
type ringHashPicker struct {
	mu    sync.Mutex
	peers []*peer
	ring  []vnode
	rot   uint64
	now   func() time.Time
}

func newRingHashPicker(peers []*peer, now func() time.Time) *ringHashPicker {
	rp := &ringHashPicker{peers: peers, now: now}
	for i, pe := range peers {
		replicas := pe.weight() * vnodeFactor
		for j := 0; j < replicas; j++ {
			rp.ring = append(rp.ring, vnode{
				hash: fnv1a32(pe.tg.Name + "#" + strconv.Itoa(j)),
				idx:  i,
			})
		}
	}
	sort.Slice(rp.ring, func(a, b int) bool {
		if rp.ring[a].hash != rp.ring[b].hash {
			return rp.ring[a].hash < rp.ring[b].hash
		}
		return rp.ring[a].idx < rp.ring[b].idx
	})
	return rp
}

func (p *ringHashPicker) Pick(hashKey string) (*Target, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	n := uint64(len(p.peers))
	if hashKey == "" {
		for off := uint64(0); off < n; off++ {
			i := int((p.rot + off) % n)
			pe := p.peers[i]
			if pe.weight() <= 0 || !pe.available(now) {
				continue
			}
			p.rot = (p.rot + 1) % maxU64(n, 1)
			if pe.begin(now) {
				return pe.tg, nil
			}
		}
		p.rot = (p.rot + 1) % maxU64(n, 1)
		return nil, ErrNoHealthy
	}
	h := fnv1a32(hashKey)
	pos := sort.Search(len(p.ring), func(i int) bool { return p.ring[i].hash >= h })
	excluded := make([]bool, len(p.peers))
	for k := 0; k < len(p.ring); k++ {
		vn := p.ring[(pos+k)%len(p.ring)]
		if excluded[vn.idx] {
			continue
		}
		excluded[vn.idx] = true
		pe := p.peers[vn.idx]
		if pe.weight() <= 0 || !pe.available(now) {
			continue
		}
		if pe.begin(now) {
			return pe.tg, nil
		}
	}
	return nil, ErrNoHealthy
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func fnv1a32(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}
