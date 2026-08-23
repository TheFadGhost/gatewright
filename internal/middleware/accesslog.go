package middleware

import (
	"net/http"
	"time"

	"gatewright/internal/obs"
)

// captureWriter records status code and bytes written for the access line.
type captureWriter struct {
	http.ResponseWriter
	code        int
	bytesOut    int64
	wroteHeader bool
}

func (c *captureWriter) WriteHeader(code int) {
	if c.wroteHeader {
		return
	}
	c.wroteHeader, c.code = true, code
	c.ResponseWriter.WriteHeader(code)
}

func (c *captureWriter) Write(b []byte) (int, error) {
	if !c.wroteHeader {
		c.WriteHeader(http.StatusOK)
	}
	n, err := c.ResponseWriter.Write(b)
	c.bytesOut += int64(n)
	return n, err
}

func (c *captureWriter) status() int {
	if !c.wroteHeader {
		return http.StatusOK // Go's implicit default for handlers that never write
	}
	return c.code
}

// NewAccessLog starts the per-request timer, installs (or reuses) the Record
// accumulator, and emits exactly one access line after the inner chain
// finishes — including when it panics; the panic is re-thrown afterwards so
// the outermost recovery still converts it to INT500.
func NewAccessLog(logger obs.Logger, route string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := RecordFrom(r.Context())
			if rec == nil {
				rec = &Record{}
				r = r.WithContext(WithRecord(r.Context(), rec))
			}
			sw := &captureWriter{ResponseWriter: w}
			var thrown any
			func() {
				defer func() {
					thrown = recover()
					f := rec.Fields
					f.TS = time.Now().UTC().Format(time.RFC3339Nano)
					f.Method = r.Method
					f.Path = r.URL.Path
					f.Query = r.URL.RawQuery
					f.Status = sw.status()
					f.Remote = r.RemoteAddr
					f.ReqID = RequestIDFrom(r.Context())
					f.DurationMS = float64(time.Since(start).Microseconds()) / 1000.0
					if r.ContentLength > 0 {
						f.BytesIn = r.ContentLength
					}
					f.BytesOut = sw.bytesOut
					if route != "" {
						f.Route = route
					}
					logger.Access(f)
				}()
				next.ServeHTTP(sw, r)
			}()
			RecordStage(r.Context(), OrderNames[PosAccessLog], time.Since(start))
			if thrown != nil {
				panic(thrown)
			}
		})
	}
}
