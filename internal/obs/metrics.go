package obs

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Metrics is Gatewright's metrics registry emitting the Prometheus text
// exposition format (v0.0.4). Naming rules (DESIGN.md §5):
//
//	gatewright_<noun>_<unit>; counters end _total; histograms end _seconds.
type Metrics struct {
	mu        sync.Mutex
	counters  map[string]*counterFamily
	gauges    map[string]*gaugeFamily
	histograms map[string]*histogramFamily
}

func NewMetrics() *Metrics {
	return &Metrics{
		counters:   map[string]*counterFamily{},
		gauges:     map[string]*gaugeFamily{},
		histograms: map[string]*histogramFamily{},
	}
}

type labelSet map[string]string

func keyFor(name string, labels labelSet) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteString(`="`)
		b.WriteString(escapeLabel(labels[k]))
		b.WriteString(`"`)
	}
	b.WriteByte('}')
	return b.String()
}

func escapeLabel(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(v)
}

type counterFamily struct {
	help   string
	values map[string]float64 // series key -> value
}

type gaugeFamily struct {
	help   string
	values map[string]float64
}

// DefaultBuckets cover 1ms..10s for gateway latencies.
var DefaultBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
	0.25, 0.5, 1, 2.5, 5, 10}

type histogramFamily struct {
	help    string
	buckets []float64 // upper bounds, ascending; +Inf implicit
	series  map[string]*histSeries
}

type histSeries struct {
	counts []uint64 // per bucket cumulative counts are computed at render
	sum    float64
	count  uint64
	bucketCounts []uint64
}

// IncCounter increments a counter series by 1.
func (m *Metrics) IncCounter(name, help string, labels labelSet) { m.AddCounter(name, help, labels, 1) }

// AddCounter increments a counter series by v.
func (m *Metrics) AddCounter(name, help string, labels labelSet, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.counters[name]
	if !ok {
		f = &counterFamily{help: help, values: map[string]float64{}}
		m.counters[name] = f
	}
	f.values[keyFor(name, labels)] += v
}

// SetGauge sets a gauge series.
func (m *Metrics) SetGauge(name, help string, labels labelSet, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.gauges[name]
	if !ok {
		f = &gaugeFamily{help: help, values: map[string]float64{}}
		m.gauges[name] = f
	}
	f.values[keyFor(name, labels)] = v
}

// Observe records one histogram observation in seconds.
func (m *Metrics) Observe(name, help string, buckets []float64, labels labelSet, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.histograms[name]
	if !ok {
		if len(buckets) == 0 {
			buckets = DefaultBuckets
		}
		f = &histogramFamily{help: help, buckets: buckets, series: map[string]*histSeries{}}
		m.histograms[name] = f
	}
	k := keyFor(name, labels)
	s, ok := f.series[k]
	if !ok {
		s = &histSeries{bucketCounts: make([]uint64, len(f.buckets))}
		f.series[k] = s
	}
	s.sum += v
	s.count++
	for i, ub := range f.buckets {
		if v <= ub {
			s.bucketCounts[i]++
		}
	}
}

// Render produces the full Prometheus text exposition.
func (m *Metrics) Render() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var b strings.Builder
	writeSection := func(typ, name, help string, emit func(sb *strings.Builder)) {
		b.WriteString("# HELP " + name + " " + help + "\n")
		b.WriteString("# TYPE " + name + " " + typ + "\n")
		emit(&b)
	}

	names := make([]string, 0, len(m.counters))
	for n := range m.counters {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		f := m.counters[n]
		writeSection("counter", n, f.help, func(sb *strings.Builder) {
			keys := sortedKeys(f.values)
			for _, k := range keys {
				writeFloat(sb, k, f.values[k])
			}
		})
	}

	names = names[:0]
	for n := range m.gauges {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		f := m.gauges[n]
		writeSection("gauge", n, f.help, func(sb *strings.Builder) {
			for _, k := range sortedKeys(f.values) {
				writeFloat(sb, k, f.values[k])
			}
		})
	}

	names = names[:0]
	for n := range m.histograms {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		f := m.histograms[n]
		b.WriteString("# HELP " + n + " " + f.help + "\n")
		b.WriteString("# TYPE " + n + " histogram\n")
		baseName := strings.TrimSuffix(n, "_seconds")
		keys := make([]string, 0, len(f.series))
		for k := range f.series {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			s := f.series[k]
			labelPart := seriesLabels(k)
			cum := uint64(0)
			for i, ub := range f.buckets {
				_ = i
				cum = s.bucketCounts[i]
				sb := labelWithBucket(labelPart, formatFloat(ub))
				writeUintLine(&b, baseName+"_seconds_bucket"+sb, cum)
			}
			writeUintLine(&b, baseName+"_seconds_bucket"+labelWithBucket(labelPart, "+Inf"), s.count)
			writeFloat(&b, baseName+"_seconds_sum"+labelPart, s.sum)
			writeUintLine(&b, baseName+"_seconds_count"+labelPart, s.count)
		}
	}
	return b.String()
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func writeFloat(b *strings.Builder, key string, v float64) {
	b.WriteString(key)
	b.WriteByte(' ')
	b.WriteString(formatFloat(v))
	b.WriteByte('\n')
}

func writeUintLine(b *strings.Builder, key string, v uint64) {
	b.WriteString(key)
	b.WriteByte(' ')
	b.WriteString(formatUint(v))
	b.WriteByte('\n')
}

func seriesLabels(seriesKey string) string {
	i := strings.IndexByte(seriesKey, '{')
	if i < 0 {
		return ""
	}
	return seriesKey[i:]
}

func labelWithBucket(labelPart, bucket string) string {
	if labelPart == "" {
		return "{le=\"" + bucket + "\"}"
	}
	return labelPart[:len(labelPart)-1] + `,le="` + bucket + `"}`
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func formatUint(v uint64) string {
	return strconv.FormatUint(v, 10)
}

// Handler serves /metrics with correct content type.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte(m.Render()))
	})
}
