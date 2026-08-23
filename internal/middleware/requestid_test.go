package middleware

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

var gwIDPattern = regexp.MustCompile(`^gw-[A-Z2-7]{26}$`)

func TestRequestIDGeneratedWhenAbsent(t *testing.T) {
	var seen string
	h := NewRequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFrom(r.Context())
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)
	if !gwIDPattern.MatchString(seen) {
		t.Fatalf("generated id %q does not match gw-<26 base32 chars>", seen)
	}
	if got := req.Header.Get(RequestIDHeader); got != seen {
		t.Fatalf("header %q != ctx %q", got, seen)
	}
}

func TestRequestIDsAreUnique(t *testing.T) {
	seen := map[string]bool{}
	var h http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := RequestIDFrom(r.Context())
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	})
	mw := NewRequestID()
	for i := 0; i < 100; i++ {
		mw(h).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}
	if len(seen) != 100 {
		t.Fatalf("%d unique ids out of 100", len(seen))
	}
}

func TestRequestIDValidInboundPassedThrough(t *testing.T) {
	for _, id := range []string{"abc-123_XYZ.9", strings.Repeat("a", 64), "0"} {
		var ctxID string
		h := NewRequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctxID = RequestIDFrom(r.Context())
		}))
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set(RequestIDHeader, id)
		h.ServeHTTP(httptest.NewRecorder(), req)
		if ctxID != id || req.Header.Get(RequestIDHeader) != id {
			t.Fatalf("valid id %q not preserved (ctx=%q header=%q)", id, ctxID, req.Header.Get(RequestIDHeader))
		}
	}
}

func TestRequestIDInvalidInboundRegenerated(t *testing.T) {
	bad := []string{
		"",
		strings.Repeat("x", 65),
		"has space",
		"line\nbreak",
		"tab\there",
		"\x01control",
		"del\x7f",
		"unicode-é", // é is non-ASCII
	}
	for _, in := range bad {
		if ValidRequestID(in) {
			t.Fatalf("ValidRequestID(%q) = true, want false", in)
		}
		var got string
		h := NewRequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = RequestIDFrom(r.Context())
		}))
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if in != "" {
			req.Header.Set(RequestIDHeader, in)
		}
		h.ServeHTTP(httptest.NewRecorder(), req)
		if got == in || !gwIDPattern.MatchString(got) {
			t.Fatalf("invalid id %q: got %q, want fresh generated id", in, got)
		}
	}
}

func TestValidRequestIDBoundaries(t *testing.T) {
	cases := map[string]bool{
		"a":                     true,
		strings.Repeat("z", 64): true,
		strings.Repeat("z", 65): false,
		"":                      false,
		" ":                     false, // 0x20 excluded
		"~":                     true,  // 0x7e printable edge
	}
	for in, want := range cases {
		if got := ValidRequestID(in); got != want {
			t.Fatalf("ValidRequestID(%q) = %v, want %v", in, got, want)
		}
	}
}
