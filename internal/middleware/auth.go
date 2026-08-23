package middleware

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	jwt "github.com/golang-jwt/jwt/v5"

	"gatewright/internal/config"
	"gatewright/internal/errs"
)

// Identity is the authenticated principal, stored in the request context and
// consumed by key extractors (api_key) and downstream stages.
type Identity struct {
	APIKey  string
	Subject string
	Claims  map[string]any
}

type identityCtxKey struct{}

// WithIdentity installs id into ctx.
func WithIdentity(ctx context.Context, id *Identity) context.Context {
	return context.WithValue(ctx, identityCtxKey{}, id)
}

// IdentityFrom returns the request identity; ok is false when unauthenticated
// (auth type none, or the stage has not run yet).
func IdentityFrom(ctx context.Context) (*Identity, bool) {
	id, ok := ctx.Value(identityCtxKey{}).(*Identity)
	return id, ok
}

// NewAuth builds the auth stage. Misconfiguration (unknown type, unusable key
// source, missing secret/JWKS for the configured algorithm families) fails
// fast at startup: per DESIGN.md §3 configuration errors never become HTTP
// responses.
func NewAuth(a *config.Auth) Middleware {
	if a == nil || a.Type == "" || a.Type == "none" {
		return func(next http.Handler) http.Handler { return next }
	}
	switch a.Type {
	case "api_key":
		return newAPIKeyAuth(a.APIKey)
	case "jwt":
		return newJWTAuth(a.JWT)
	default:
		panic(fmt.Sprintf("middleware: unknown auth.type %q (known: none, api_key, jwt)", a.Type))
	}
}

func unauthorized(w http.ResponseWriter, r *http.Request, challenge string) {
	recordCode(r.Context(), errs.CodeUnauthorized)
	w.Header().Set("WWW-Authenticate", challenge)
	errs.WriteWithID(w,
		errs.New(errs.CodeUnauthorized, "missing or invalid credentials"),
		RequestIDFrom(r.Context()))
}

// ---------------------------------------------------------------------------
// api_key
// ---------------------------------------------------------------------------

const defaultAPIKeyHeader = "X-API-Key"

// loadAPIKeys reads the key material once at startup. KeysEnv wins when both
// sources are configured. Entries are comma/newline separated, trimmed, and
// empty entries dropped.
func loadAPIKeys(envName, filePath string) ([]string, error) {
	var raw string
	switch {
	case envName != "":
		v, ok := os.LookupEnv(envName)
		if !ok {
			return nil, fmt.Errorf("keys_env %q is not set", envName)
		}
		raw = v
	case filePath != "":
		b, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("keys_file %q: %w", filePath, err)
		}
		raw = string(b)
	default:
		return nil, errors.New("one of keys_env or keys_file is required")
	}
	split := func(r rune) bool { return r == ',' || r == '\n' || r == '\r' }
	var keys []string
	for _, part := range strings.FieldsFunc(raw, split) {
		if k := strings.TrimSpace(part); k != "" {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil, errors.New("no usable api keys after parsing")
	}
	return keys, nil
}

// keyDigestMatch compares in constant time across the whole key set:
// candidates and keys are hashed first so every comparison is 32 bytes.
func keyDigestMatch(candidate string, digests [][sha256.Size]byte) bool {
	sum := sha256.Sum256([]byte(candidate))
	match := 0
	for i := range digests {
		match |= subtle.ConstantTimeCompare(sum[:], digests[i][:])
	}
	return match == 1
}

func newAPIKeyAuth(a *config.APIKeyAuth) Middleware {
	if a == nil {
		panic(`middleware: auth.type "api_key" requires an api_key block`)
	}
	header := a.Header
	if header == "" {
		header = defaultAPIKeyHeader
	}
	keys, err := loadAPIKeys(a.KeysEnv, a.KeysFile)
	if err != nil {
		panic("middleware: api_key auth: " + err.Error())
	}
	digests := make([][sha256.Size]byte, 0, len(keys))
	for _, k := range keys {
		digests = append(digests, sha256.Sum256([]byte(k)))
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			cand := r.Header.Get(header)
			if cand == "" || !keyDigestMatch(cand, digests) {
				unauthorized(w, r, `ApiKey realm="gatewright"`)
				return
			}
			ctx := WithIdentity(r.Context(), &Identity{APIKey: cand})
			next.ServeHTTP(w, r.WithContext(ctx))
			RecordStage(ctx, OrderNames[PosAuth], time.Since(start))
		})
	}
}

// ---------------------------------------------------------------------------
// jwt
// ---------------------------------------------------------------------------

func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", false
	}
	scheme, rest, found := strings.Cut(h, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	tok := strings.TrimSpace(rest)
	return tok, tok != ""
}

// newJWTAuth enforces the algorithm whitelist twice: via jwt.WithValidMethods
// (rejects before key selection — this kills alg=none) and again inside the
// key function. HS* uses SecretEnv; RS*/ES* resolve through the JWKS URL with
// keyfunc/v3's refreshing storage. exp is required and validated by the
// parser; issuer/audience are enforced when configured.
func newJWTAuth(a *config.JWTAuth) Middleware {
	if a == nil {
		panic(`middleware: auth.type "jwt" requires a jwt block`)
	}
	if len(a.Algorithms) == 0 {
		panic("middleware: jwt auth: algorithms whitelist must not be empty")
	}
	hs, asym := false, false
	for _, al := range a.Algorithms {
		switch {
		case strings.HasPrefix(al, "HS"):
			hs = true
		case strings.HasPrefix(al, "RS"), strings.HasPrefix(al, "ES"):
			asym = true
		default:
			panic("middleware: jwt auth: unsupported algorithm " + al)
		}
	}
	var secret []byte
	if hs {
		s := os.Getenv(a.SecretEnv)
		if s == "" {
			panic("middleware: jwt auth: secret_env " + a.SecretEnv + " is not set or empty")
		}
		secret = []byte(s)
	}
	var jwksFn func(*jwt.Token) (any, error)
	if asym {
		if a.JwksURL == "" {
			panic("middleware: jwt auth: jwks_url is required for RS*/ES* algorithms")
		}
		k, err := keyfunc.NewDefault([]string{a.JwksURL})
		if err != nil {
			panic("middleware: jwt auth: jwks setup failed: " + err.Error())
		}
		jwksFn = k.Keyfunc
	}
	allowed := make(map[string]bool, len(a.Algorithms))
	for _, al := range a.Algorithms {
		allowed[al] = true
	}
	keyFn := func(t *jwt.Token) (any, error) {
		alg, _ := t.Header["alg"].(string)
		if !allowed[alg] {
			return nil, fmt.Errorf("algorithm %q is not in the configured whitelist", alg)
		}
		if strings.HasPrefix(alg, "HS") {
			return secret, nil
		}
		return jwksFn(t)
	}
	opts := []jwt.ParserOption{
		jwt.WithValidMethods(a.Algorithms),
		jwt.WithExpirationRequired(),
	}
	if a.Issuer != "" {
		opts = append(opts, jwt.WithIssuer(a.Issuer))
	}
	if a.Audience != "" {
		opts = append(opts, jwt.WithAudience(a.Audience))
	}
	parser := jwt.NewParser(opts...)
	const challenge = `Bearer realm="gatewright"`
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			tokStr, ok := bearerToken(r)
			if !ok {
				unauthorized(w, r, challenge)
				return
			}
			tk, err := parser.Parse(tokStr, keyFn)
			if err != nil || tk == nil || !tk.Valid {
				unauthorized(w, r, challenge+`, error="invalid_token"`)
				return
			}
			mc, _ := tk.Claims.(jwt.MapClaims)
			id := &Identity{Claims: mc}
			if s, ok := mc["sub"].(string); ok {
				id.Subject = s
			}
			next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), id)))
			RecordStage(r.Context(), OrderNames[PosAuth], time.Since(start))
		})
	}
}
