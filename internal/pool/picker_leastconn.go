package pool

import (
	"sync"
	"time"
)

// leastConnPicker selects the eligible peer with the fewest in-flight
// requests. Ties prefer the higher weight; remaining ties are resolved by a
// deterministic rotating start position so equal peers receive traffic in
// round-robin order.
type leastConnPicker struct {
	mu    sync.Mutex
	peers []*peer
	rot   uint64
	now   func() time.Time
}

func newLeastConnPicker(peers []*peer, now func() time.Time) *leastConnPicker {
	return &leastConnPicker{peers: peers, now: now}
}

func (p *leastConnPicker) Pick(_ string) (*Target, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	n := len(p.peers)
	excluded := make([]bool, n)
	for {
		best := -1
		var bestInflight int64
		bestRank, bestDist := 0, 0
		found := false
		for off := 0; off < n; off++ {
			i := int((uint64(off) + p.rot) % uint64(n))
			if excluded[i] {
				continue
			}
			pe := p.peers[i]
			w := pe.weight()
			if w <= 0 || !pe.available(now) {
				continue
			}
			inf := pe.inflight()
			rank := -w
			switch {
			case !found:
			case inf < bestInflight:
			case inf > bestInflight:
				continue
			case rank < bestRank:
			case rank > bestRank:
				continue
			case off >= bestDist:
				continue
			}
			best, bestInflight, bestRank, bestDist, found = i, inf, rank, off, true
		}
		if !found {
			return nil, ErrNoHealthy
		}
		p.rot++
		if p.peers[best].begin(now) {
			return p.peers[best].tg, nil
		}
		excluded[best] = true
	}
}
