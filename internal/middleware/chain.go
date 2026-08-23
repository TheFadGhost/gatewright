package middleware

import "net/http"

// Middleware wraps a handler with one pipeline stage (DESIGN.md §6).
type Middleware func(http.Handler) http.Handler

// Chain applies mws around h so that mws[0] becomes the outermost stage: a
// request traverses the middlewares in order, the response unwinds in reverse.
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		if mws[i] == nil {
			continue
		}
		h = mws[i](h)
	}
	return h
}
