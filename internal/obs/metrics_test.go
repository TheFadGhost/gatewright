package obs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// renderGolden is the exact, deterministic exposition of the registry built
// by newGoldenMetrics. Family order is counters, gauges, histograms; names
// and label series are sorted lexicographically inside each block.
func newGoldenMetrics() *Metrics {
	m := NewMetrics()
	// Registered out of alphabetical order on purpose.
	m.IncCounter("gatewright_requests_total", "Requests proxied.",
		labelSet{"code": "200"})
	m.IncCounter("gatewright_requests_total", "Requests proxied.",
		labelSet{"code": "200"})
	m.AddCounter("gatewright_requests_total", "Requests proxied.",
		labelSet{"code": "500"}, 1)
	m.SetGauge("gatewright_active_conns", "Active connections.",
		labelSet{"route": "b"}, 0)
	m.IncCounter("gatewright_auth_total", "Auth decisions.",
		labelSet{"kind": "key"})
	m.SetGauge("gatewright_active_conns", "Active connections.",
		labelSet{"route": "a"}, 7)

	// Observations are dyadic so the rendered sum is exact.
	for _, v := range []float64{0.0625, 0.25, 2} {
		m.Observe("gatewright_upstream_latency_seconds", "Upstream latency.",
			latencyBuckets, labelSet{"route": "api"}, v)
	}
	return m
}

var latencyBuckets = []float64{0.1, 0.5, 1}

const renderGolden = `# HELP gatewright_auth_total Auth decisions.
# TYPE gatewright_auth_total counter
gatewright_auth_total{kind="key"} 1
# HELP gatewright_requests_total Requests proxied.
# TYPE gatewright_requests_total counter
gatewright_requests_total{code="200"} 2
gatewright_requests_total{code="500"} 1
# HELP gatewright_active_conns Active connections.
# TYPE gatewright_active_conns gauge
gatewright_active_conns{route="a"} 7
gatewright_active_conns{route="b"} 0
# HELP gatewright_upstream_latency_seconds Upstream latency.
# TYPE gatewright_upstream_latency_seconds histogram
gatewright_upstream_latency_seconds_bucket{route="api",le="0.1"} 1
gatewright_upstream_latency_seconds_bucket{route="api",le="0.5"} 2
gatewright_upstream_latency_seconds_bucket{route="api",le="1"} 2
gatewright_upstream_latency_seconds_bucket{route="api",le="+Inf"} 3
gatewright_upstream_latency_seconds_sum{route="api"} 2.3125
gatewright_upstream_latency_seconds_count{route="api"} 3
`

func TestRenderDeterministicGoldenOutput(t *testing.T) {
	m := newGoldenMetrics()
	got := m.Render()
	if got != renderGolden {
		t.Fatalf("Render() mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, renderGolden)
	}
	if again := m.Render(); again != got {
		t.Error("Render() must be deterministic across calls")
	}
}

func TestRenderFamilyAndSeriesOrdering(t *testing.T) {
	out := newGoldenMetrics().Render()

	iCounter := strings.Index(out, "# TYPE gatewright_requests_total counter")
	iGauge := strings.Index(out, "# TYPE gatewright_active_conns gauge")
	iHist := strings.Index(out, "# TYPE gatewright_upstream_latency_seconds histogram")
	if !(0 <= iCounter && iCounter < iGauge && iGauge < iHist) {
		t.Fatalf("family order wrong: counter=%d gauge=%d histogram=%d", iCounter, iGauge, iHist)
	}
	if !strings.Contains(out, "# HELP gatewright_requests_total Requests proxied.\n") {
		t.Error("HELP line text wrong")
	}
	// Label series sorted deterministically (code=200 before code=500).
	if strings.Index(out, `code="200"`) > strings.Index(out, `code="500"`) {
		t.Error("counter series not sorted")
	}
}

func TestHistogramBucketSumCountMath(t *testing.T) {
	m := NewMetrics()
	m.Observe("h_seconds", "Latency.", []float64{0.25, 0.5, 1},
		labelSet{"route": "r1"}, 0.125)
	m.Observe("h_seconds", "Latency.", []float64{0.25, 0.5, 1},
		labelSet{"route": "r1"}, 0.5)
	m.Observe("h_seconds", "Latency.", []float64{0.25, 0.5, 1},
		labelSet{"route": "r1"}, 2)

	want := `# HELP h_seconds Latency.
# TYPE h_seconds histogram
h_seconds_bucket{route="r1",le="0.25"} 1
h_seconds_bucket{route="r1",le="0.5"} 2
h_seconds_bucket{route="r1",le="1"} 2
h_seconds_bucket{route="r1",le="+Inf"} 3
h_seconds_sum{route="r1"} 2.625
h_seconds_count{route="r1"} 3
`
	if got := m.Render(); got != want {
		t.Fatalf("histogram exposition:\n got:\n%s\nwant:\n%s", got, want)
	}
}

func TestHistogramWithoutLabelsUsesLeOnly(t *testing.T) {
	m := NewMetrics()
	m.Observe("bare_seconds", "Bare.", []float64{1}, nil, 0.5)
	m.Observe("bare_seconds", "Bare.", []float64{1}, nil, 4)
	want := `# HELP bare_seconds Bare.
# TYPE bare_seconds histogram
bare_seconds_bucket{le="1"} 1
bare_seconds_bucket{le="+Inf"} 2
bare_seconds_sum 4.5
bare_seconds_count 2
`
	if got := m.Render(); got != want {
		t.Fatalf("label-free histogram:\n got:\n%s\nwant:\n%s", got, want)
	}
}

func TestLabelValuesEscaped(t *testing.T) {
	m := NewMetrics()
	// Value characters: a " b \ c LF d \ e — every escapable byte appears,
	// including a backslash directly before the newline.
	m.SetGauge("escape_test", "Escaping.",
		labelSet{"lbl": "a\"b\\c\nd\\e"}, 5)
	wantLine := `escape_test{lbl="a\"b\\c\nd\\e"} 5`
	if out := m.Render(); !strings.Contains(out, wantLine+"\n") {
		t.Errorf("escaped line missing.\nwant %q\nin:\n%s", wantLine, out)
	}
}

func TestHandlerServesExpositionWithContentType(t *testing.T) {
	m := NewMetrics()
	m.IncCounter("served_total", "Served.", labelSet{"code": "200"})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d", rec.Code)
	}
	wantCT := "text/plain; version=0.0.4; charset=utf-8"
	if got := rec.Header().Get("Content-Type"); got != wantCT {
		t.Errorf("Content-Type = %q, want %q", got, wantCT)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q", got)
	}
	if body := rec.Body.String(); body != m.Render() {
		t.Errorf("handler body differs from Render():\n%q\nvs\n%q", body, m.Render())
	}
}

func TestDefaultBucketsUsedWhenNoneSupplied(t *testing.T) {
	m := NewMetrics()
	m.Observe("auto_seconds", "Auto.", nil, labelSet{"p": "q"}, 0.004)
	out := m.Render()
	for _, bound := range DefaultBuckets {
		if !strings.Contains(out, `le="`+formatFloat(bound)+`"`) {
			t.Errorf("default bucket %v missing from output:\n%s", bound, out)
		}
	}
}
