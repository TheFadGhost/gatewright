package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBodyLimitContentLengthRejected(t *testing.T) {
	reached := false
	h := NewBodyLimit(16)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		_, _ = w.Write([]byte("nope"))
	}))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(strings.Repeat("x", 17)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if reached {
		t.Fatal("inner handler ran despite oversized Content-Length")
	}
	env := decodeEnvelope(t, rec)
	switch {
	case rec.Code != http.StatusRequestEntityTooLarge:
		t.Errorf("status = %d, want 413", rec.Code)
	case env.Error.Code != "BODY001":
		t.Errorf("code = %q, want BODY001", env.Error.Code)
	}
}

func TestBodyLimitAtExactBoundaryAllowed(t *testing.T) {
	body := strings.Repeat("y", 16)
	h := NewBodyLimit(16)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read at boundary failed: %v", err)
		}
		_, _ = w.Write(b)
	}))
	req := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != body {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}

func TestBodyLimitStreamedOverLimitRejected(t *testing.T) {
	// Chunked-style request: unknown ContentLength forces detection during read.
	payload := strings.Repeat("z", 64)
	h := NewBodyLimit(16)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		if err == nil {
			t.Error("expected read error on over-limit stream")
			w.WriteHeader(http.StatusOK)
			return
		}
		// Handler correctly aborts without writing a status.
	}))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
	req.ContentLength = -1 // simulate chunked transfer encoding
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	env := decodeEnvelope(t, rec)
	switch {
	case rec.Code != http.StatusRequestEntityTooLarge:
		t.Errorf("status = %d, want 413", rec.Code)
	case env.Error.Code != "BODY001":
		t.Errorf("code = %q, want BODY001", env.Error.Code)
	}
}

func TestBodyLimitStreamedWithinLimitPasses(t *testing.T) {
	payload := strings.Repeat("ok", 8)
	h := NewBodyLimit(1024)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("unexpected read error: %v", err)
		}
		_, _ = w.Write(b)
	}))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != payload {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}

func TestBodyLimitUnlimited(t *testing.T) {
	payload := strings.Repeat("big", 100000)
	h := NewBodyLimit(-1)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Errorf("unexpected error with unlimited body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_ = n
	}))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestBodyLimitHandlerWritingDespiteReadErrorStill413(t *testing.T) {
	// A handler that ignores the read error and writes anyway must not leak a
	// success response past the limit stage.
	h := NewBodyLimit(4)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		_ = err // ignored on purpose
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("pretend it worked"))
	}))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("way more than four bytes"))
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body=%q", rec.Code, rec.Body.String())
	}
}

func TestBodyLimitEnvelopeCarriesReqID(t *testing.T) {
	logger := &captureLogger{}
	h := Chain(
		okHandler("unreachable"),
		NewRequestID(),
		NewAccessLog(logger, "r"),
		NewBodyLimit(2),
	)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("too long"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	env := decodeEnvelope(t, rec)
	if env.Error.ReqID == "" {
		t.Fatal("envelope missing req_id")
	}
	if !strings.HasPrefix(env.Error.ReqID, "gw-") && len(env.Error.ReqID) > 64 {
		t.Fatalf("suspicious req_id %q", env.Error.ReqID)
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.fields) != 1 || logger.fields[0].Code != "BODY001" {
		t.Fatalf("access line should carry code BODY001, got %+v", logger.fields)
	}
}
