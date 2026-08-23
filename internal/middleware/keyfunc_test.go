package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"gatewright/internal/config"
)

func mustExtractor(t *testing.T, spec string) KeyExtractor {
	t.Helper()
	ks, err := config.ParseKeySpec(spec)
	if err != nil {
		t.Fatalf("ParseKeySpec(%q): %v", spec, err)
	}
	fn, err := BuildKeyExtractor(*ks)
	if err != nil {
		t.Fatalf("BuildKeyExtractor(%q): %v", spec, err)
	}
	return fn
}

func TestBuildKeyExtractorRejectsEmpty(t *testing.T) {
	if _, err := BuildKeyExtractor(config.KeySpec{}); err == nil {
		t.Fatal("expected error for empty spec")
	}
}

func TestKeyExtractorIPStripsPort(t *testing.T) {
	fn := mustExtractor(t, "ip")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.7:52344"
	got := fn(req, nil)
	if got != "ip\x0010.0.0.7" {
		t.Fatalf("key = %q, want ip\\x0010.0.0.7", got)
	}
	req.RemoteAddr = "weird-no-port"
	if got := fn(req, nil); got != "ip\x00weird-no-port" {
		t.Fatalf("key without port = %q", got)
	}
}

func TestKeyExtractorPathAndHeader(t *testing.T) {
	pathFn := mustExtractor(t, "path")
	req := httptest.NewRequest(http.MethodGet, "/v1/users/42?q=1", nil)
	if got := pathFn(req, nil); got != "path\x00/v1/users/42" {
		t.Fatalf("path key = %q (query must not leak in)", got)
	}
	hdrFn := mustExtractor(t, "header:X-Api-Key")
	req.Header.Set("X-API-Key", "k-123")
	if got := hdrFn(req, nil); got != "header\x00k-123" {
		t.Fatalf("header key = %q", got)
	}
	if got := hdrFn(httptest.NewRequest(http.MethodGet, "/", nil), nil); got != "header\x00" {
		t.Fatalf("missing header key = %q", got)
	}
}

func TestKeyExtractorAPIKeyUsesIdentity(t *testing.T) {
	fn := mustExtractor(t, "api_key")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if got := fn(req, nil); got != "api_key\x00" {
		t.Fatalf("nil identity key = %q", got)
	}
	if got := fn(req, &Identity{APIKey: "sk-live"}); got != "api_key\x00sk-live" {
		t.Fatalf("identity key = %q", got)
	}
}

func TestKeyExtractorCompositeOrderStable(t *testing.T) {
	fn := mustExtractor(t, "composite[ip,path]")
	req := httptest.NewRequest(http.MethodGet, "/v1/items", nil)
	req.RemoteAddr = "192.168.1.2:80"
	want := "ip\x00192.168.1.2\x00path\x00/v1/items"
	if got := fn(req, nil); got != want {
		t.Fatalf("composite = %q, want %q", got, want)
	}
	// Declared order is preserved: swapping parts swaps the key.
	fn2 := mustExtractor(t, "composite[path,ip]")
	want2 := "path\x00/v1/items\x00ip\x00192.168.1.2"
	if got := fn2(req, nil); got != want2 {
		t.Fatalf("composite reversed = %q, want %q", got, want2)
	}
}

func TestKeyExtractorCompositeFourParts(t *testing.T) {
	fn := mustExtractor(t, "composite[api_key,ip,header:X-Tenant,path]")
	req := httptest.NewRequest(http.MethodGet, "/p", nil)
	req.RemoteAddr = "[::1]:9999"
	req.Header.Set("X-Tenant", "acme")
	id := &Identity{APIKey: "kk"}
	want := "api_key\x00kk\x00ip\x00::1\x00header\x00acme\x00path\x00/p"
	if got := fn(req, id); got != want {
		t.Fatalf("composite4 = %q, want %q", got, want)
	}
}
