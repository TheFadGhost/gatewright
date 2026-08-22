// Command gatewright runs the Gatewright reverse-proxy API gateway.
package main

import (
	"fmt"
	"os"
)

const version = "0.0.0-dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version":
		fmt.Println("gatewright", version)
	case "run":
		runCmd(os.Args[2:])
	case "validate":
		validateCmd(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `gatewright - a readable reverse-proxy API gateway with pluggable rate limiting

Usage:
  gatewright run -c <config.yaml>        start the gateway
  gatewright validate -c <config.yaml>   validate config; exits 1 on error
  gatewright version                     print version
`)
}
