package main

import (
	"bytes"
	"fmt"
	"os"

	"gatewright/internal/config"
	"gopkg.in/yaml.v3"
)

func configLoad(path string) (*config.Config, *config.ValidationError) {
	return config.Load(path)
}

func validateCmd(args []string) {
	fs := flagSet("validate")
	cfgPath := fs.String("c", "gateway.yaml", "configuration file")
	diffPath := fs.String("diff", "", "compare against this older config and print the change set")
	fs.Parse(args)

	if *diffPath != "" {
		runDiff(*cfgPath, *diffPath)
	}

	if cfg, verr := configLoad(*cfgPath); verr != nil {
		fmt.Fprint(os.Stderr, verr.Error())
		os.Exit(1)
	} else {
		limiters := 0
		for i := range cfg.Routes {
			limiters += len(cfg.Routes[i].RateLimits)
		}
		fmt.Printf("%s: OK (%d routes, %d pools, %d limiters)\n",
			*cfgPath, len(cfg.Routes), len(cfg.Upstreams), limiters)
	}
}

func runDiff(newPath, oldPath string) {
	newNode, nerr := parseYAMLFile(newPath)
	oldNode, oerr := parseYAMLFile(oldPath)
	if nerr != nil || oerr != nil {
		return
	}
	var changes []string
	diffMapping(oldNode, newNode, "", &changes)
	for _, c := range changes {
		fmt.Println(c)
	}
	if len(changes) == 0 {
		fmt.Println("no configuration changes")
	}
}

func parseYAMLFile(path string) (*yaml.Node, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot read %s: %v\n", path, err)
		os.Exit(1)
	}
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		fmt.Fprintf(os.Stderr, "invalid YAML in %s: %v\n", path, err)
		os.Exit(1)
	}
	if len(node.Content) == 0 {
		return nil, fmt.Errorf("empty document")
	}
	return node.Content[0], nil
}

// diffMapping walks two YAML trees reporting added/removed/changed scalar
// leaves with their exact dotted paths. Sequences compare by index.
func diffMapping(oldN, newN *yaml.Node, path string, out *[]string) {
	switch {
	case oldN == nil && newN == nil:
		return
	case oldN == nil:
		*out = append(*out, "+ "+pathOrRoot(path)+" = "+scalarOf(newN))
		return
	case newN == nil:
		*out = append(*out, "- "+pathOrRoot(path)+" (was "+scalarOf(oldN)+")")
		return
	}
	if oldN.Kind != yaml.MappingNode || newN.Kind != yaml.MappingNode {
		if !bytes.Equal([]byte(scalarOf(oldN)), []byte(scalarOf(newN))) {
			*out = append(*out, "~ "+pathOrRoot(path)+": "+scalarOf(oldN)+" -> "+scalarOf(newN))
		}
		return
	}
	oldKeys := map[string]*yaml.Node{}
	for i := 0; i+1 < len(oldN.Content); i += 2 {
		oldKeys[oldN.Content[i].Value] = oldN.Content[i+1]
	}
	newKeys := map[string]*yaml.Node{}
	for i := 0; i+1 < len(newN.Content); i += 2 {
		k := newN.Content[i].Value
		v := newN.Content[i+1]
		newKeys[k] = v
		child := joinKey(path, k)
		if ov, ok := oldKeys[k]; ok {
			diffValues(ov, v, child, out)
		} else {
			collectLeaves(v, child, "+ ", out)
		}
	}
	for k, ov := range oldKeys {
		if _, ok := newKeys[k]; !ok {
			collectLeaves(ov, joinKey(path, k), "- ", out)
		}
	}
}

func diffValues(oldN, newN *yaml.Node, path string, out *[]string) {
	if oldN.Kind == yaml.SequenceNode && newN.Kind == yaml.SequenceNode {
		maxLen := len(oldN.Content)
		if len(newN.Content) > maxLen {
			maxLen = len(newN.Content)
		}
		for i := 0; i < maxLen; i++ {
			var ov, nv *yaml.Node
			if i < len(oldN.Content) {
				ov = oldN.Content[i]
			}
			if i < len(newN.Content) {
				nv = newN.Content[i]
			}
			diffMapping(ov, nv, fmt.Sprintf("%s[%d]", path, i), out)
		}
		return
	}
	diffMapping(oldN, newN, path, out)
}

// collectLeaves records every scalar leaf under a subtree with a prefix tag.
func collectLeaves(n *yaml.Node, path, tag string, out *[]string) {
	switch n.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			collectLeaves(n.Content[i+1], joinKey(path, n.Content[i].Value), tag, out)
		}
	case yaml.SequenceNode:
		for i, item := range n.Content {
			collectLeaves(item, fmt.Sprintf("%s[%d]", path, i), tag, out)
		}
	default:
		*out = append(*out, tag+pathOrRoot(path)+" = "+n.Value)
	}
}

func scalarOf(n *yaml.Node) string {
	if n == nil {
		return "(absent)"
	}
	if n.Kind == yaml.ScalarNode {
		return n.Value
	}
	return "(" + mapKind(n.Kind) + ")"
}

func mapKind(k yaml.Kind) string {
	switch k {
	case yaml.MappingNode:
		return "mapping"
	case yaml.SequenceNode:
		return "sequence"
	default:
		return "node"
	}
}

func pathOrRoot(p string) string {
	if p == "" {
		return "(root)"
	}
	return p
}

func joinKey(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}
