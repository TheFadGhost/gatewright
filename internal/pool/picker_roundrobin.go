package pool

import (
	"sync"
	"time"
)

// roundRobinPicker implements smooth weighted round-robin (nginx-style):
// every pass adds each eligible peer's weight to its current weight, selects
// the peer with the highest current weight and subtracts the total from it,
// which yields exact weight-proportional distribution over full cycles.
type roundRobinPicker struct {
	mu    sync.Mutex
	peers []*peer
	cur   []int
	now   func() time.Time
}

func newRoundRobinPicker(peers []*peer, now func() time.Time) *roundRobinPicker {
	return &roundRobinPicker{peers: peers, cur: make([]int, len(peers)), now: now}
}

func (p *roundRobinPicker) Pick(_ string) (*Target, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	excluded := make([]bool, len(p.peers))
	for {
		best := -1
		total := 0
		for i, pe := range p.peers {
			if excluded[i] {
				continue
			}
			w := pe.weight()
			if w <= 0 || !pe.available(now) {
				continue
			}
			total += w
			p.cur[i] += w
			if best == -1 || p.cur[i] > p.cur[best] {
				best = i
			}
		}
		if best == -1 {
			return nil, ErrNoHealthy
		}
		p.cur[best] -= total
		if p.peers[best].begin(now) {
			return p.peers[best].tg, nil
		}
		p.cur[best] += total
		excluded[best] = true
	}
}
