package admin

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"gatewright/internal/errs"
)

const publishInterval = time.Second

// broker fans out marshalled state snapshots to SSE subscribers. Slow clients
// drop frames instead of blocking publishers.
type broker struct {
	mu   sync.Mutex
	next int
	subs map[int]chan []byte
}

func newBroker() *broker {
	return &broker{subs: make(map[int]chan []byte)}
}

func (b *broker) subscribe() (int, <-chan []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.next++
	ch := make(chan []byte, 16)
	b.subs[b.next] = ch
	return b.next, ch
}

func (b *broker) unsubscribe(id int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.subs[id]; ok {
		delete(b.subs, id)
		close(ch)
	}
}

func (b *broker) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}

func (b *broker) publish(msg []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subs {
		select {
		case ch <- msg:
		default:
			// Slow subscriber: skip this frame rather than stall the loop.
		}
	}
}

// ensureLoop starts the 1 s snapshot publisher when the first client connects
// and stops it when the last one leaves.
func (s *Server) ensureLoop() {
	s.loopMu.Lock()
	defer s.loopMu.Unlock()
	if s.loopStop == nil {
		ctx, cancel := context.WithCancel(context.Background())
		s.loopStop = cancel
		go s.stateLoop(ctx)
	}
}

func (s *Server) maybeStopLoop() {
	s.loopMu.Lock()
	defer s.loopMu.Unlock()
	if s.loopStop != nil && s.broker.count() == 0 {
		s.loopStop()
		s.loopStop = nil
	}
}

func (s *Server) stateLoop(ctx context.Context) {
	ticker := time.NewTicker(publishInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.broker.count() == 0 {
				continue
			}
			msg := s.stateBytes()
			s.setLastState(msg)
			s.broker.publish(msg)
		}
	}
}

// handleEvents streams the dashboard state as server-sent events: an initial
// hello event, then a state event immediately and once per second after.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAdminError(w, http.StatusInternalServerError, errs.CodeInternal, "streaming unsupported")
		return
	}
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")

	if _, err := fmt.Fprint(w, "retry: 3000\n\n"); err != nil {
		return
	}
	if _, err := fmt.Fprint(w, "event: hello\ndata: {}\n\n"); err != nil {
		return
	}
	flusher.Flush()

	id, ch := s.broker.subscribe()
	defer func() {
		s.broker.unsubscribe(id)
		s.maybeStopLoop()
	}()
	s.ensureLoop()

	if first := s.lastStateBytes(); first != nil {
		if !writeSSEState(w, first) {
			return
		}
		flusher.Flush()
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case msg, open := <-ch:
			if !open {
				return
			}
			if !writeSSEState(w, msg) {
				return
			}
			flusher.Flush()
		}
	}
}

func writeSSEState(w http.ResponseWriter, data []byte) bool {
	_, err := fmt.Fprintf(w, "event: state\ndata: %s\n\n", data)
	return err == nil
}
