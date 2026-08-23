package middleware

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"

	"gatewright/internal/config"
)

func apiKeyCfg(env string) *config.Auth {
	return &config.Auth{Type: "api_key", APIKey: &config.APIKeyAuth{KeysEnv: env}}
}

func doAuth(h http.Handler, header, value string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if value != "" {
		req.Header.Set(header, value)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAPIKeyEnvLoadedOnce(t *testing.T) {
	t.Setenv("GW_TEST_KEYS", " alpha , beta\n,gamma,,")
	h := NewAuth(apiKeyCfg("GW_TEST_KEYS"))(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := IdentityFrom(r.Context())
		if !ok || id.APIKey == "" {
			t.Error("identity not stored on success")
		}
		w.WriteHeader(http.StatusOK)
	}))
	for _, key := range []string{"alpha", "beta", "gamma"} {
		if rec := doAuth(h, "X-API-Key", key); rec.Code != http.StatusOK {
			t.Fatalf("key %q rejected with %d", key, rec.Code)
		}
	}
}

func TestAPIKeyFileMultiFormat(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "keys.txt")
	content := "key-a\nkey-b, key-c ,\r\n\n  key-d  \n,"
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	h := NewAuth(&config.Auth{
		Type:   "api_key",
		APIKey: &config.APIKeyAuth{KeysFile: file},
	})(okHandler("ok"))
	for _, key := range []string{"key-a", "key-b", "key-c", "key-d"} {
		if rec := doAuth(h, "X-API-Key", key); rec.Code != http.StatusOK {
			t.Fatalf("file key %q rejected with %d", key, rec.Code)
		}
	}
	if rec := doAuth(h, "X-API-Key", "key-z"); rec.Code != http.StatusUnauthorized {
		t.Fatal("unknown file key accepted")
	}
}

func TestAPIKeyMissingAndWrongAreAUTH001(t *testing.T) {
	t.Setenv("GW_TEST_KEYS", "right-key")
	h := NewAuth(apiKeyCfg("GW_TEST_KEYS"))(okHandler("never"))
	rec := doAuth(h, "X-API-Key", "")
	env := decodeEnvelope(t, rec)
	switch {
	case rec.Code != http.StatusUnauthorized:
		t.Errorf("missing key status = %d, want 401", rec.Code)
	case env.Error.Code != "AUTH001":
		t.Errorf("missing key code = %q, want AUTH001", env.Error.Code)
	case !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "ApiKey"):
		t.Errorf("WWW-Authenticate = %q", rec.Header().Get("WWW-Authenticate"))
	}
	rec = doAuth(h, "X-API-Key", "wrong-key")
	env = decodeEnvelope(t, rec)
	switch {
	case rec.Code != http.StatusUnauthorized:
		t.Errorf("wrong key status = %d", rec.Code)
	case env.Error.Code != "AUTH001":
		t.Errorf("wrong key code = %q (AUTH002 reserved for scopes)", env.Error.Code)
	}
}

func TestAPIKeyCustomHeader(t *testing.T) {
	t.Setenv("GW_TEST_KEYS", "kkk")
	h := NewAuth(&config.Auth{
		Type:   "api_key",
		APIKey: &config.APIKeyAuth{Header: "X-Tenant-Token", KeysEnv: "GW_TEST_KEYS"},
	})(okHandler("ok"))
	if rec := doAuth(h, "X-Tenant-Token", "kkk"); rec.Code != http.StatusOK {
		t.Fatalf("custom header rejected: %d", rec.Code)
	}
	if rec := doAuth(h, "X-API-Key", "kkk"); rec.Code != http.StatusUnauthorized {
		t.Fatal("default header must not satisfy custom header config")
	}
}

func TestAPIKeysWithDifferentLengthsMatchExactly(t *testing.T) {
	t.Setenv("GW_TEST_KEYS", "short, a-much-longer-key-value")
	h := NewAuth(apiKeyCfg("GW_TEST_KEYS"))(okHandler("ok"))
	cases := map[string]int{"short": 200, "a-much-longer-key-value": 200, "shor": 401, "": 401,
		"a-much-longer-key-valu": 401, strings.Repeat("x", 500): 401}
	for cand, want := range cases {
		if got := doAuth(h, "X-API-Key", cand).Code; got != want {
			t.Fatalf("candidate %q -> %d, want %d", cand, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// JWT (HS256 family from secret env)
// ---------------------------------------------------------------------------

const testSecret = "test-secret-material"

func hsToken(t *testing.T, method jwt.SigningMethod, claims jwt.MapClaims, key any) string {
	t.Helper()
	s, err := jwt.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return s
}

func baseClaims() jwt.MapClaims {
	return jwt.MapClaims{"sub": "user-42", "iss": "https://issuer.example",
		"aud": "gatewright", "exp": time.Now().Add(time.Hour).Unix()}
}

func newJWTHS(algs []string, mutate func(*config.JWTAuth)) http.Handler {
	cfg := &config.Auth{Type: "jwt", JWT: &config.JWTAuth{
		SecretEnv:  "GW_TEST_JWT_SECRET",
		Issuer:     "https://issuer.example",
		Audience:   "gatewright",
		Algorithms: algs,
	}}
	if mutate != nil {
		mutate(cfg.JWT)
	}
	var inner http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		id, _ := IdentityFrom(r.Context())
		_ = json.NewEncoder(w).Encode(id)
	}
	return NewAuth(cfg)(inner)
}

func bearerReq(tok string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	return req
}

func TestJWTValidTokenPassesWithSubject(t *testing.T) {
	t.Setenv("GW_TEST_JWT_SECRET", testSecret)
	h := newJWTHS([]string{"HS256"}, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, bearerReq(hsToken(t, jwt.SigningMethodHS256, baseClaims(), []byte(testSecret))))
	if rec.Code != http.StatusOK {
		t.Fatalf("valid token rejected: %d %s", rec.Code, rec.Body.String())
	}
	var id Identity
	if err := json.Unmarshal(rec.Body.Bytes(), &id); err != nil {
		t.Fatal(err)
	}
	switch {
	case id.Subject != "user-42":
		t.Errorf("subject = %q", id.Subject)
	case id.Claims["iss"] != "https://issuer.example":
		t.Errorf("iss claim = %v", id.Claims["iss"])
	}
}

func TestJWTExpiredRejected(t *testing.T) {
	t.Setenv("GW_TEST_JWT_SECRET", testSecret)
	h := newJWTHS([]string{"HS256"}, nil)
	c := baseClaims()
	c["exp"] = time.Now().Add(-time.Minute).Unix()
	rec := doAuth(h, "Authorization", "Bearer "+hsToken(t, jwt.SigningMethodHS256, c, []byte(testSecret)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expired token status = %d, want 401", rec.Code)
	}
	if env := decodeEnvelope(t, rec); env.Error.Code != "AUTH001" {
		t.Fatalf("code = %q", env.Error.Code)
	}
}

func TestJWTMissingExpRejected(t *testing.T) {
	t.Setenv("GW_TEST_JWT_SECRET", testSecret)
	h := newJWTHS([]string{"HS256"}, nil)
	c := baseClaims()
	delete(c, "exp")
	rec := doAuth(h, "Authorization", "Bearer "+hsToken(t, jwt.SigningMethodHS256, c, []byte(testSecret)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("token without exp accepted (%d); exp is REQUIRED", rec.Code)
	}
}

func TestJWTWrongIssuerAndAudienceRejected(t *testing.T) {
	t.Setenv("GW_TEST_JWT_SECRET", testSecret)
	h := newJWTHS([]string{"HS256"}, nil)
	c := baseClaims()
	c["iss"] = "https://evil.example"
	if rec := doAuth(h, "Authorization", "Bearer "+hsToken(t, jwt.SigningMethodHS256, c, []byte(testSecret))); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong issuer accepted: %d", rec.Code)
	}
	c = baseClaims()
	c["aud"] = "other-service"
	if rec := doAuth(h, "Authorization", "Bearer "+hsToken(t, jwt.SigningMethodHS256, c, []byte(testSecret))); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong audience accepted: %d", rec.Code)
	}
}

func TestJWTAlgNoneRejected(t *testing.T) {
	t.Setenv("GW_TEST_JWT_SECRET", testSecret)
	h := newJWTHS([]string{"HS256"}, nil)
	tok := hsToken(t, jwt.SigningMethodNone, baseClaims(), jwt.UnsafeAllowNoneSignatureType)
	rec := doAuth(h, "Authorization", "Bearer "+tok)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("alg=none accepted with status %d", rec.Code)
	}
}

func TestJWTAlgorithmWhitelistEnforced(t *testing.T) {
	t.Setenv("GW_TEST_JWT_SECRET", testSecret)
	h := newJWTHS([]string{"HS256"}, nil)
	tok := hsToken(t, jwt.SigningMethodHS512, baseClaims(), []byte(testSecret))
	if rec := doAuth(h, "Authorization", "Bearer "+tok); rec.Code != http.StatusUnauthorized {
		t.Fatalf("HS512 accepted while whitelist is HS256: %d", rec.Code)
	}
}

func TestJWTWrongSignatureRejected(t *testing.T) {
	t.Setenv("GW_TEST_JWT_SECRET", testSecret)
	h := newJWTHS([]string{"HS256"}, nil)
	rec := doAuth(h, "Authorization", "Bearer "+hsToken(t, jwt.SigningMethodHS256, baseClaims(), []byte("attacker-secret")))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("forged signature accepted: %d", rec.Code)
	}
}

func TestJWTMissingAuthorizationHeader(t *testing.T) {
	t.Setenv("GW_TEST_JWT_SECRET", testSecret)
	h := newJWTHS([]string{"HS256"}, nil)
	rec := doAuth(h, "Authorization", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing Authorization -> %d", rec.Code)
	}
	if got := rec.Header().Get("WWW-Authenticate"); !strings.Contains(got, "Bearer") {
		t.Fatalf("WWW-Authenticate = %q", got)
	}
}

// ---------------------------------------------------------------------------
// JWT RS256 via a local JWKS endpoint (exercises the keyfunc/v3 wiring)
// ---------------------------------------------------------------------------

type jwksServer struct {
	srv   *httptest.Server
	priv  *rsa.PrivateKey // trusted signing key published in the JWK set
	wrong *rsa.PrivateKey // untrusted key of the same shape
	kid   string
	url   string
}

func startJWKS(t *testing.T) *jwksServer {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	kid := "gatewright-test-key"
	jwk := map[string]any{
		"kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256",
		"n": base64.RawURLEncoding.EncodeToString(priv.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(priv.E)).Bytes()),
	}
	body, _ := json.Marshal(map[string]any{"keys": []any{jwk}})
	mux := http.NewServeMux()
	mux.HandleFunc("/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &jwksServer{srv: srv, priv: priv, wrong: other, kid: kid, url: srv.URL + "/jwks.json"}
}

func rsToken(t *testing.T, priv *rsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = kid
	s, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("rs sign: %v", err)
	}
	return s
}

func TestJWTRS256ViaJWKS(t *testing.T) {
	js := startJWKS(t)
	cfg := &config.Auth{Type: "jwt", JWT: &config.JWTAuth{
		JwksURL: js.url, Issuer: "https://issuer.example", Algorithms: []string{"RS256"},
	}}
	var seenSubject string
	h := NewAuth(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, _ := IdentityFrom(r.Context())
		seenSubject = id.Subject
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, bearerReq(rsToken(t, js.priv, js.kid, baseClaims())))
	if rec.Code != http.StatusOK {
		t.Fatalf("valid RS256 token rejected: %d %s", rec.Code, rec.Body.String())
	}
	if seenSubject != "user-42" {
		t.Fatalf("subject = %q", seenSubject)
	}
	// Token signed by an untrusted key with the same shape must fail.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, bearerReq(rsToken(t, js.wrong, js.kid, baseClaims())))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("untrusted RS256 key accepted: %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// none / misconfiguration
// ---------------------------------------------------------------------------

func TestAuthNonePassthroughNoIdentityRequired(t *testing.T) {
	h := NewAuth(&config.Auth{Type: "none"})(okHandler("open"))
	if rec := doAuth(h, "X-API-Key", ""); rec.Code != http.StatusOK {
		t.Fatalf("auth none blocked a request: %d", rec.Code)
	}
	if h := NewAuth(nil)(okHandler("open")); rec2Code(h) != http.StatusOK {
		t.Fatal("nil auth config should pass through")
	}
}

func rec2Code(h http.Handler) int {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	return rec.Code
}

func mustPanic(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s did not panic at construction", what)
		}
	}()
	fn()
}

func TestAuthMisconfigurationFailsFast(t *testing.T) {
	mustPanic(t, "api_key without source", func() {
		NewAuth(&config.Auth{Type: "api_key", APIKey: &config.APIKeyAuth{}})
	})
	mustPanic(t, "jwt without algorithms", func() {
		NewAuth(&config.Auth{Type: "jwt", JWT: &config.JWTAuth{SecretEnv: "X"}})
	})
	mustPanic(t, "unknown type", func() {
		NewAuth(&config.Auth{Type: "oauth9"})
	})
}
