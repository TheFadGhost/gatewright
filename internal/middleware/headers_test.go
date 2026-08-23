package middleware

import (
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
