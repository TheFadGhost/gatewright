package admin

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"gatewright/internal/errs"
)

// percentileWindow is the rolling window providers compute percentiles over,
// matching the dashboard chart specification in DESIGN.md section 7.
const percentileWindow = 5 * time.Minute

// Options configures the admin server.
type Options struct {
	// AuthToken enables bearer auth when non-empty. An empty token is only
	// acceptable when the listener is bound to loopback; that bind policy is
	// enforced by the caller, not here.
	AuthToken string
	// Dashboard controls whether the embedded UI is served under /admin/.
	Dashboard bool
}

// Server implements http.Handler for the admin API and dashboard.
type Server struct {
	sp     SnapshotProvider
	opts   Options
	mux    *http.ServeMux
	broker *broker

	loopMu   sync.Mutex
	loopStop func()

	stateMu   sync.Mutex
	lastState []byte
}

// New builds the admin server around sp.
func New(sp SnapshotProvider, opts Options) *Server {
	s := &Server{
		sp:     sp,
		opts:   opts,
		mux:    http.NewServeMux(),
		broker: newBroker(),
	}
	s.mux.HandleFunc("GET /admin/api/state", s.handleState)
	s.mux.HandleFunc("POST /admin/api/reload", s.handleReload)
	s.mux.HandleFunc("GET /admin/api/metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /admin/events", s.handleEvents)
	if opts.Dashboard {
		s.mux.HandleFunc("GET /admin/assets/", handleAsset)
		s.mux.HandleFunc("GET /admin/{$}", handleIndex)
		s.mux.HandleFunc("GET /admin", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/admin/", http.StatusMovedPermanently)
		})
	}
	return s
}

// ServeHTTP applies authentication then dispatches to the admin mux.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	s.withAuth(s.mux).ServeHTTP(w, r)
}

func (s *Server) buildState() StateDTO {
	routes := s.sp.RoutesView()
	rates := s.sp.RequestRates()
	dto := StateDTO{
		Version:       s.sp.Version(),
		UptimeSeconds: s.sp.Uptime().Seconds(),
		GeneratedAt:   time.Now().UTC(),
		Routes:        make([]RouteStateDTO, 0, len(routes)),
		Pools:         []PoolDTO{},
		Limiters:      []LimiterView{},
	}
	for _, rv := range routes {
		row := RouteStateDTO{RouteView: rv}
		row.RPS = rates[rv.Name]
		p50, p95, p99, ok := s.sp.LatencyPercentiles(rv.Name, percentileWindow)
		row.Percentiles = PercentileDTO{OK: ok}
		if ok {
			row.Percentiles.P50Ms = &p50
			row.Percentiles.P95Ms = &p95
			row.Percentiles.P99Ms = &p99
		}
		dto.Routes = append(dto.Routes, row)
	}
	for _, pv := range s.sp.PoolsView() {
		dto.Pools = append(dto.Pools, poolToDTO(pv))
	}
	dto.Limiters = append(dto.Limiters, s.sp.LimiterViews()...)
	return dto
}

func (s *Server) stateBytes() []byte {
	b, err := json.Marshal(s.buildState())
	if err != nil {
		log.Printf("admin: marshal state: %v", err)
		return []byte(`{"version":"","uptime_seconds":0,"routes":[],"pools":[],"limiters":[]}`)
	}
	return b
}

func (s *Server) setLastState(b []byte) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.lastState = b
}

func (s *Server) lastStateBytes() []byte {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.lastState
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, code, message string) {
	errs.Write(w, errs.New(code, message))
}

// writeAdminError emits the canonical error envelope with an explicit status,
// used where errs.HTTPStatus's mapping does not match the admin semantics.
func writeAdminError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error errs.APIError `json:"error"`
	}{Error: *errs.New(code, message)})
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.buildState())
}

func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	if err := s.sp.Reload(); err != nil {
		writeAdminError(w, http.StatusInternalServerError, errs.CodeInternal, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(s.sp.MetricsText()))
}
