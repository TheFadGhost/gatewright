package middleware

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"gatewright/internal/obs"
)

// captureLogger implements obs.Logger, recording Access calls.
type captureLogger struct {
	mu     sync.Mutex
	fields []obs.AccessFields
}

func (l *captureLogger) Access(f obs.AccessFields) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fields = append(l.fields, f)
}

func (l *captureLogger) Info(string, ...any)  {}
func (l *captureLogger) Warn(string, ...any)  {}
func (l *captureLogger) Error(string, ...any) {}
func (l *captureLogger) Writer() io.Writer    { return io.Discard }

func TestAccessLogBasicFields(t *testing.T) {
	logger := &captureLogger{}
	h := NewAccessLog(logger, "api-v1")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/users/42?verbose=1", nil)
	req.RemoteAddr = "203.0.113.9:52344"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.fields) != 1 {
		t.Fatalf("access lines = %d, want exactly 1", len(logger.fields))
	}
	f := logger.fields[0]
	switch {
	case f.Method != http.MethodPost:
		t.Errorf("method = %q", f.Method)
	case f.Path != "/v1/users/42":
		t.Errorf("path = %q", f.Path)
	case f.Query != "verbose=1":
		t.Errorf("query = %q", f.Query)
	case f.Status != http.StatusCreated:
		t.Errorf("status = %d, want 201", f.Status)
	case f.Remote != "203.0.113.9:52344":
		t.Errorf("remote = %q", f.Remote)
	case f.Route != "api-v1":
		t.Errorf("route = %q", f.Route)
	case f.DurationMS < 0:
		t.Errorf("duration_ms = %v", f.DurationMS)
	case f.BytesOut != int64(len(`{"ok":true}`)):
		t.Errorf("bytes_out = %d", f.BytesOut)
	}
	if _, err := time.Parse(time.RFC3339Nano, f.TS); err != nil {
		t.Errorf("ts not RFC3339Nano: %v", err)
	}
}

func TestAccessLogDefaultStatus200(t *testing.T) {
	logger := &captureLogger{}
	h := NewAccessLog(logger, "r")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.fields) != 1 || logger.fields[0].Status != http.StatusOK {
		t.Fatalf("implicit status not captured as 200: %+v", logger.fields)
	}
}

func TestAccessLogRecordEnrichmentFromLaterStages(t *testing.T) {
	logger := &captureLogger{}
	proxySimulator := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if rec := RecordFrom(r.Context()); rec != nil {
				rec.Fields.Route = "sim-route"
				rec.Fields.Upstream = "catalog"
				rec.Fields.UpstreamAddr = "127.0.0.1:9001"
			}
			next.ServeHTTP(w, r)
		})
	}
	// access-log outermost, enrichment stage inside it
	h := NewAccessLog(logger, "")(Middleware(proxySimulator)(okHandler("x")))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	logger.mu.Lock()
	defer logger.mu.Unlock()
	f := logger.fields[0]
	switch {
	case f.Route != "sim-route":
		t.Errorf("route = %q, want enriched value", f.Route)
	case f.Upstream != "catalog" || f.UpstreamAddr != "127.0.0.1:9001":
		t.Errorf("upstream fields = %q/%q", f.Upstream, f.UpstreamAddr)
	}
}

func TestAccessLogEmitsOnceAndRethrowsOnPanic(t *testing.T) {
	logger := &captureLogger{}
	h := NewAccessLog(logger, "r")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}()
	switch {
	case recovered == nil:
		t.Fatal("panic was swallowed; outermost recovery would never see it")
	case fmt.Sprint(recovered) != "boom":
		t.Fatalf("re-thrown panic altered: %v", recovered)
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.fields) != 1 {
		t.Fatalf("access lines on panic = %d, want exactly 1", len(logger.fields))
	}
}
