// Package errs defines the stable error taxonomy shared by the proxy, admin API,
// CLI and logs. Codes are part of Gatewright's public surface: never renumber.
package errs

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// ErrStorePathImmutable is returned when a reload attempts to change
// store.path: the shared limiter database cannot be swapped at runtime, so
// such reloads are rejected until the process is restarted.
var ErrStorePathImmutable = errors.New("store.path changes require a restart")

// Configuration errors (never returned over HTTP; they fail load/validate).
const (
	CodeUnknownField      = "CFG001"
	CodeInvalidValue      = "CFG002"
	CodeMissingRequired   = "CFG003"
	CodeDuplicateName     = "CFG004"
	CodeSemanticConflict  = "CFG005"
	CodeUnsafeCombination = "CFG006"
)

// Request/response errors (returned to clients with a stable JSON envelope).
const (
	CodeNoRoute           = "RT001"
	CodeMethodNotAllowed  = "RT002"
	CodeInvalidPath       = "RT003"
	CodeUnauthorized      = "AUTH001"
	CodeForbidden         = "AUTH002"
	CodeRateLimited       = "RATE001"
	CodePayloadTooLarge   = "BODY001"
	CodeConnectTimeout    = "UP001"
	CodeReadTimeout       = "UP002"
	CodeWriteTimeout      = "UP003"
	CodeTotalTimeout      = "UP004"
	CodeBadGateway        = "UP010"
	CodeCircuitOpen       = "UP011"
	CodeNoHealthyUpstream = "UP012"
	CodeInternal          = "INT500"
)

// HTTPStatus maps an error code to its response status. Unknown codes are 500.
func HTTPStatus(code string) int {
	switch code {
	case CodeNoRoute:
		return http.StatusNotFound
	case CodeMethodNotAllowed:
		return http.StatusMethodNotAllowed
	case CodeInvalidPath:
		return http.StatusBadRequest
	case CodeUnauthorized:
		return http.StatusUnauthorized
	case CodeForbidden:
		return http.StatusForbidden
	case CodeRateLimited:
		return http.StatusTooManyRequests
	case CodePayloadTooLarge:
		return http.StatusRequestEntityTooLarge
	case CodeConnectTimeout, CodeReadTimeout, CodeWriteTimeout, CodeTotalTimeout:
		return http.StatusGatewayTimeout
	case CodeCircuitOpen, CodeNoHealthyUpstream:
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadGateway
	}
}

// APIError is the wire shape: {"error":{"code","message","req_id"}}.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	ReqID   string `json:"req_id,omitempty"`
}

type envelope struct {
	Error APIError `json:"error"`
}

// New builds an APIError.
func New(code, message string) *APIError { return &APIError{Code: code, Message: message} }

func (e *APIError) Error() string {
	if e.ReqID != "" {
		return fmt.Sprintf("%s: %s (req_id=%s)", e.Code, e.Message, e.ReqID)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Write emits the canonical error envelope. Content-Type is always application/json.
func Write(w http.ResponseWriter, apiErr *APIError) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	status := HTTPStatus(apiErr.Code)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(envelope{Error: *apiErr})
}

// WriteWithID is Write with the request id attached.
func WriteWithID(w http.ResponseWriter, apiErr *APIError, reqID string) {
	apiErr.ReqID = reqID
	Write(w, apiErr)
}
