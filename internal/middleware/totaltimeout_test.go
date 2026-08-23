package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gatewright/internal/errs"
)

func TestTotalTimeoutDeadlineBeforeWriteYieldsUP004(t *testing.T) {
	rec := newSyncRecorder()
	h := NewTotalTimeout(40 * time.Millisecond)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block until the deadline fires, then finish without writing.
		select {
		case <-r.Context().Done():
			time.Sleep(5 * time.Millisecond) // let the watcher goroutine win the race
		case <-time.After(2 * time.Second):
			t.Error("deadline never observed")
		}
	}))
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	code, body := awaitStatus(rec, errs.HTTPStatus(errs.CodeTotalTimeout), 3*time.Second)
	if code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", code)
	}
	if !strings.Contains(body, "UP004") {
		t.Fatalf("body missing UP004: %q", body)
	}
}

func TestTotalTimeoutFastHandlerPasses(t *testing.T) {
	h := NewTotalTimeout(5 * time.Second)(okHandler("fast"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "fast" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}

func TestTotalTimeoutResponseStartedNotCorrupted(t *testing.T) {
	h := NewTotalTimeout(30 * time.Millisecond)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-First", "1")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("early"))
		time.Sleep(80 * time.Millisecond) // outlive the deadline
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	time.Sleep(60 * time.Millisecond) // watcher fires after response started; must be a no-op
	switch {
	case rec.Code != http.StatusAccepted:
		t.Fatalf("status = %d, want 202 (no UP004 after response started)", rec.Code)
	case rec.Body.String() != "early":
		t.Fatalf("body = %q, want %q", rec.Body.String(), "early")
	case rec.Header().Get("Content-Type") == "application/json":
		t.Fatal("UP004 envelope headers leaked into started response")
	}
}

func TestTotalTimeoutInstallsDeadline(t *testing.T) {
	var dlSet bool
	var within time.Duration
	h := NewTotalTimeout(time.Second)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dl, ok := r.Context().Deadline()
		dlSet = ok
		within = time.Until(dl)
	}))
	start := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	elapsed := time.Since(start)
	if !dlSet {
		t.Fatal("no deadline on request context")
	}
	if within > time.Second || within < time.Second-elapsed-10*time.Millisecond {
		t.Fatalf("deadline not ~1s away: %v", within)
	}
}

func TestTotalTimeoutDisabledWhenZero(t *testing.T) {
	called := false
	h := NewTotalTimeout(0)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.Context().Deadline(); ok {
			t.Error("unexpected deadline with d=0")
		}
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if !called {
		t.Fatal("handler not reached")
	}
}

func TestTotalTimeoutRecordCodeInAccessLine(t *testing.T) {
	logger := &captureLogger{}
	rec := newSyncRecorder()
	h := Chain(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
			time.Sleep(5 * time.Millisecond)
		}),
		NewRequestID(),
		NewAccessLog(logger, "t"),
		NewTotalTimeout(30*time.Millisecond),
	)
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	awaitStatus(rec, http.StatusGatewayTimeout, 3*time.Second)
	time.Sleep(20 * time.Millisecond)
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.fields) != 1 || logger.fields[0].Code != "UP004" {
		t.Fatalf("expected one access line coded UP004, got %+v", logger.fields)
	}
}
