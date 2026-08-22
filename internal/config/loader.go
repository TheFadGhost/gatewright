package config

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Load reads, decodes and validates a configuration file. On problems it
// returns a nil Config and a *ValidationError listing EVERY error, each naming
// its exact path (DESIGN.md §3).
func Load(path string) (*Config, *ValidationError) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, &ValidationError{File: path, Errors: []*Error{{
			File: path, Path: "(file)", Expected: "readable file", Found: err.Error(),
		}}}
	}
	return Parse(data, path)
}

// Parse validates configuration from bytes; used by tests and stdin mode.
func Parse(data []byte, file string) (*Config, *ValidationError) {
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		line, col := yamlPos(err.Error())
		return nil, &ValidationError{File: file, Errors: []*Error{{
			File: file, Line: line, Column: col, Path: "(document)",
			Expected: "valid YAML", Found: firstLine(err.Error()),
		}}}
	}
	if len(root.Content) == 0 || root.Content[0].Kind != yaml.MappingNode {
		return nil, &ValidationError{File: file, Errors: []*Error{{
			File: file, Path: "(document)", Code: CodeMissingRequired,
			Expected: "a non-empty YAML mapping", Found: describeNode(&root),
		}}}
	}

	doc := root.Content[0]
	var errs []*Error

	// 1. Unknown-key detection with exact paths (reflection over schema tags).
	cfgType := reflect.TypeOf(Config{})
	checkMapping(doc, cfgType, "", file, &errs, 0)

	// 2. Strict typed decode (catches type mismatches inside custom scalars,
	// which record their own positions for the validation walk).
	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		for _, iss := range decodeErrs(err) {
			errs = append(errs, iss.toError(file))
		}
	}

	cfg.SourceFile = file

	// 3. Semantic validation + defaults; attaches paths to scalar errors too.
	if verr := cfg.normalizeAndValidate(); verr != nil {
		errs = append(errs, verr.Errors...)
	}

	if len(errs) > 0 {
		sortErrors(errs)
		return nil, &ValidationError{File: file, Errors: errs}
	}
	return &cfg, nil
}

var errCodeUnknownKey = CodeUnknownField

// checkMapping validates one YAML mapping against its Go struct type.
func checkMapping(node *yaml.Node, t reflect.Type, path, file string, errs *[]*Error, depth int) {
	if depth > 12 || len(*errs) > 64 { // cap pathological configs
		return
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if node.Kind != yaml.MappingNode || t.Kind() != reflect.Struct {
		return
	}
	if implementsYAMLUnmarshaler(t) {
		return // custom scalar: its own unmarshaler reports problems
	}

	fields := yamlFields(t)
	for i := 0; i+1 < len(node.Content); i += 2 {
		kn, vn := node.Content[i], node.Content[i+1]
		key := kn.Value
		ft, ok := fields[key]
		if !ok {
			*errs = append(*errs, &Error{
				File: file, Line: kn.Line, Column: kn.Column,
				Path:     dotted(path, key),
				Found:    strconv.Quote(key),
				Expected: "one of the documented configuration keys",
				Code:     errCodeUnknownKey,
				Hint:     "see the config reference in README.md",
			})
			continue
		}
		childPath := dotted(path, key)
		walkValue(vn, ft, childPath, file, errs, depth)
	}
}

func walkValue(vn *yaml.Node, ft reflect.Type, path, file string, errs *[]*Error, depth int) {
	for ft.Kind() == reflect.Ptr {
		ft = ft.Elem()
	}
	switch vn.Kind {
	case yaml.SequenceNode:
		if ft.Kind() == reflect.Slice && isStructKind(ft.Elem()) && !implementsYAMLUnmarshaler(ft.Elem()) {
			for idx, item := range vn.Content {
				checkMapping(item, ft.Elem(), fmt.Sprintf("%s[%d]", path, idx), file, errs, depth+1)
			}
		}
	case yaml.MappingNode:
		if ft.Kind() == reflect.Map && isStructKind(ft.Elem()) && !implementsYAMLUnmarshaler(ft.Elem()) {
			for i := 0; i+1 < len(vn.Content); i += 2 {
				kn, val := vn.Content[i], vn.Content[i+1]
				checkMapping(val, ft.Elem(), path+"."+kn.Value, file, errs, depth+1)
			}
			return
		}
		checkMapping(vn, ft, path, file, errs, depth+1)
	}
}

func isStructKind(t reflect.Type) bool { return t.Kind() == reflect.Struct }

func implementsYAMLUnmarshaler(t reflect.Type) bool {
	ptr := reflect.PtrTo(t)
	u, ok := ptr.MethodByName("UnmarshalYAML")
	return ok && u.Type.NumIn() == 2 && u.Type.In(1) == reflect.TypeOf(&yaml.Node{})
}

// yamlFields returns yaml-key -> field type for one struct type.
func yamlFields(t reflect.Type) map[string]reflect.Type {
	out := make(map[string]reflect.Type, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("yaml")
		if tag == "-" {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if name == "" {
			name = strings.ToLower(f.Name)
		}
		out[name] = f.Type
	}
	return out
}

func dotted(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}

// ---------------------------------------------------------------------------
// Typed-decode error conversion
// ---------------------------------------------------------------------------

type decodeIssue struct {
	path     string
	found    string
	expected string
	code     string
	hint     string
	line     int
	col      int
}

func (d *decodeIssue) toError(file string) *Error {
	return &Error{
		File: file, Line: d.line, Column: d.col,
		Path: d.path, Found: d.found, Expected: d.expected,
		Code: d.code, Hint: d.hint,
	}
}

var lineRe = regexp.MustCompile(`line (\d+)`)

// decodeErrs converts yaml TypeError output into located issues.
func decodeErrs(err error) []decodeIssue {
	var issues []decodeIssue
	text := err.Error()
	for _, part := range strings.Split(text, "\n") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		m := lineRe.FindStringSubmatch(part)
		line := 0
		if m != nil {
			line, _ = strconv.Atoi(m[1])
		}
		switch {
		case strings.Contains(part, "not found in type"):
			field := between(part, "field ", " not found")
			issues = append(issues, decodeIssue{
				path:     fmt.Sprintf("(line %d)", line),
				found:    strconv.Quote(field),
				expected: "a known configuration key",
				code:     CodeUnknownField,
				hint:     "remove the unknown key or check spelling in the README config reference",
				line:     line,
			})
		default:
			issues = append(issues, decodeIssue{
				path:     fmt.Sprintf("(line %d)", line),
				found:    firstLine(part),
				expected: "a value of the declared type",
				code:     CodeInvalidValue,
				line:     line,
			})
		}
	}
	return issues
}

func yamlPos(msg string) (int, int) {
	lm := regexp.MustCompile(`line (\d+)`).FindStringSubmatch(msg)
	cm := regexp.MustCompile(`column (\d+)`).FindStringSubmatch(msg)
	line, col := 0, 0
	if lm != nil {
		line, _ = strconv.Atoi(lm[1])
	}
	if cm != nil {
		col, _ = strconv.Atoi(cm[1])
	}
	return line, col
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

func between(s, start, end string) string {
	i := strings.Index(s, start)
	if i < 0 {
		return ""
	}
	s = s[i+len(start):]
	j := strings.Index(s, end)
	if j < 0 {
		return s
	}
	return s[:j]
}

func describeNode(n *yaml.Node) string {
	switch n.Kind {
	case yaml.DocumentNode:
		return "(empty document)"
	case yaml.MappingNode:
		return "mapping"
	case yaml.SequenceNode:
		return "sequence"
	case yaml.ScalarNode:
		return strconv.Quote(n.Value)
	default:
		return fmt.Sprintf("node kind %v", n.Kind)
	}
}

// sortErrors orders deterministically: known lines first ascending, then path.
func sortErrors(errs []*Error) {
	sort.SliceStable(errs, func(i, j int) bool {
		a, b := errs[i], errs[j]
		if a.Line != b.Line && a.Line != 0 && b.Line != 0 {
			return a.Line < b.Line
		}
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		return a.Code < b.Code
	})
}
