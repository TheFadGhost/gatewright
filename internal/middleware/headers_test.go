package middleware

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"gatewright/internal/config"
)

func TestRequestHeadersSetAddDel(t *testing.T) {
	m := config.HeaderManip{
		Set: map[string]string{"X-Overwrite": "new", "X-Added": "first"},
		Add: map[string]string{"X-Added": "second"},
		Del: []string{"X-Remove-Me", "x-lowercase-name"},
	}
	h := NewRequestHeaders(m)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Overwrite"); got != "new" {
			t.Errorf("set: got %q", got)
		}
		if got := r.Header.Values("X-Added"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
			t.Errorf("add: got %v", got)
		}
		if r.Header.Get("X-Remove-Me") != "" {
			t.Error("del: value survived")
		}
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Overwrite", "old")
	req.Header.Add("X-Added", "preexisting")
	req.Header.Set("X-Remove-Me", "gone-soon")
	h.ServeHTTP(httptest.NewRecorder(), req)
}

func TestRequestHeadersEmptyNoOp(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })
	h := NewRequestHeaders(config.HeaderManip{})(inner)
	if h == nil {
		t.Fatal("no-op middleware must still return a handler")
	}
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if !called {
		t.Fatal("inner not called")
	}
}

func TestRequestHeadersDelRemovesAllValues(t *testing.T) {
	m := config.HeaderManip{Del: []string{"x-multi"}}
	h := NewRequestHeaders(m)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if vals := r.Header.Values("X-Multi"); len(vals) != 0 {
			t.Errorf("del left %v", vals)
		}
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header["X-Multi"] = []string{"a", "b", "c"}
	h.ServeHTTP(httptest.NewRecorder(), req)
}

// --- response-headers stage -------------------------------------------------

func TestResponseHeadersSetAddDelAppliedBeforeWrite(t *testing.T) {
	m := config.HeaderManip{
		Set: map[string]string{"X-Server-Name": "gatewright"},
		Add: map[string]string{"X-Multi": "second"},
		Del: []string{"X-Upstream-Internal"},
	}
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream-Internal", "secret")
		w.Header().Add("X-Multi", "first")
		if got := w.Header().Get("X-Server-Name"); got != "" {
			t.Errorf("manipulation applied before inner handler ran; saw %q", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("body"))
	})
	rec := httptest.NewRecorder()
	NewResponseHeaders(m)(inner).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if got := rec.Header().Get("X-Server-Name"); got != "gatewright" {
		t.Errorf("Set = %q, want gatewright", got)
	}
	if got := rec.Header().Values("X-Multi"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Errorf("Add = %v, want [first second]", got)
	}
	if got := rec.Header().Get("X-Upstream-Internal"); got != "" {
		t.Errorf("Del failed: upstream-set header survived as %q", got)
	}
	if rec.Body.String() != "body" || rec.Code != http.StatusOK {
		t.Errorf("response body/status corrupted: %d %q", rec.Code, rec.Body.String())
	}
}

func TestResponseHeadersDelRemovesUpstreamSetHeader(t *testing.T) {
	m := config.HeaderManip{Del: []string{"X-Powered-By"}}
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Powered-By", "upstream-v9")
		w.Header().Set("X-Keep", "yes")
		_, _ = w.Write([]byte("ok"))
	})
	rec := httptest.NewRecorder()
	NewResponseHeaders(m)(inner).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := rec.Header().Get("X-Powered-By"); got != "" {
		t.Errorf("X-Powered-By = %q, want deleted before reaching the network", got)
	}
	if got := rec.Header().Get("X-Keep"); got != "yes" {
		t.Errorf("unrelated header X-Keep = %q, want preserved", got)
	}
}

func TestResponseHeadersAppliedOnceAcrossManyWrites(t *testing.T) {
	m := config.HeaderManip{Add: map[string]string{"X-Trace": "v"}}
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 5; i++ {
			_, _ = w.Write([]byte("x"))
		}
	})
	rec := httptest.NewRecorder()
	NewResponseHeaders(m)(inner).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := rec.Header().Values("X-Trace"); len(got) != 1 || got[0] != "v" {
		t.Errorf("X-Trace = %v, want exactly one application across five writes", got)
	}
	if rec.Body.String() != "xxxxx" {
		t.Errorf("body = %q, want xxxxx", rec.Body.String())
	}
}

func TestResponseHeadersEmptyNoOp(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })
	h := NewResponseHeaders(config.HeaderManip{})(inner)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if !called {
		t.Fatal("inner not called")
	}
}

func TestResponseHeadersFlushTriggersApplication(t *testing.T) {
	m := config.HeaderManip{Set: map[string]string{"X-Before-Flush": "yes"}}
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f := w.(http.Flusher)
		f.Flush() // flush BEFORE any Write/WriteHeader
		_, _ = w.Write([]byte("streamed"))
	})
	rec := httptest.NewRecorder()
	NewResponseHeaders(m)(inner).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := rec.Header().Get("X-Before-Flush"); got != "yes" {
		t.Errorf("headers not applied at Flush time: %q", got)
	}
	if rec.Body.String() != "streamed" {
		t.Errorf("body = %q", rec.Body.String())
	}
}

// hijackableRecorder satisfies http.Hijacker over an in-memory pipe so the
// passthrough path can be exercised without a real socket.
type hijackableRecorder struct {
	*httptest.ResponseRecorder
	conn net.Conn
}

func (h *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h.conn == nil {
		return nil, nil, errors.New("no hijacked conn in test")
	}
	rw := bufio.NewReadWriter(bufio.NewReader(h.conn), bufio.NewWriter(h.conn))
	return h.conn, rw, nil
}

func TestResponseHeadersHijackPassthrough(t *testing.T) {
	m := config.HeaderManip{Del: []string{"X-Secret"}, Set: map[string]string{"X-Gw": "1"}}
	client, server := net.Pipe()
	defer client.Close()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Secret", "leak-me")
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("Hijack: %v", err)
			return
		}
		_ = conn.Close()
	})
	hr := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder(), conn: server}
	NewResponseHeaders(m)(inner).ServeHTTP(hr, httptest.NewRequest(http.MethodGet, "/", nil))

	if got := hr.Header().Get("X-Secret"); got != "" {
		t.Errorf("X-Secret survived hijack path: %q", got)
	}
	if got := hr.Header().Get("X-Gw"); got != "1" {
		t.Errorf("Set not applied on hijack path: %q", got)
	}
}
