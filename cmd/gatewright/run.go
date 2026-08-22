package main

import (
	"flag"
	"fmt"
	"os"

	"gatewright/internal/config"
)

func validateCmd(args []string) {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	cfgPath := fs.String("c", "gateway.yaml", "configuration file")
	fs.Parse(args)

	if _, verr := config.Load(*cfgPath); verr != nil {
		fmt.Fprint(os.Stderr, verr.Error())
		os.Exit(1)
	}
	fmt.Printf("%s: OK\n", *cfgPath)
}

func runCmd(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	cfgPath := fs.String("c", "gateway.yaml", "configuration file")
	fs.Parse(args)

	cfg, verr := config.Load(*cfgPath)
	if verr != nil {
		fmt.Fprint(os.Stderr, verr.Error())
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "config %s loaded (%d routes) — runtime wiring pending\n",
		*cfgPath, len(cfg.Routes))
	os.Exit(3)
}
