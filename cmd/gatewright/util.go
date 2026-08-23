package main

import (
	"flag"
	"fmt"
	"os"
)

const version = "0.1.0"

func isTTY(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func flagSet(name string) *flag.FlagSet {
	return flag.NewFlagSet(name, flag.ExitOnError)
}

func osStderr() *os.File { return os.Stderr }

func osExit(code int) { os.Exit(code) }

func usage() {
	fmt.Fprint(os.Stderr, `gatewright - a readable reverse-proxy API gateway with pluggable rate limiting

Usage:
  gatewright run -c <config.yaml>              start the gateway
  gatewright validate -c <config.yaml>         validate config; exits 1 on error
  gatewright validate -c new.yaml --diff old.yaml
                                               validate and show the change set
  gatewright demo-upstream -a 127.0.0.1:9001   run the synthetic demo upstream
  gatewright version                           print version
`)
}
