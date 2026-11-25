//go:build e2e

// Command chronosperf generates Kubernetes load for Chronos and measures
// it end to end: through the apiserver, the collector and the HTTP API.
// It is test tooling, not part of the product; build it with
//
//	go build -tags e2e -o bin/chronosperf ./test/e2e/chronosperf
//
// Subcommands (run one with -h for its flags):
//
//	populate  create namespaces perf-*, ConfigMaps, Pending Pods, Deployments
//	storm     patch objects at a fixed rate, logging every write to a CSV
//	lag       join a storm CSV with Chronos history: collection lag, losses
//	bench     API latency (state, list page, full list, history, diff)
//	burst     N back-to-back updates of one object, then count records
//	recreate  delete and recreate one name N times, then check history
//	compact   compaction on a copy of a store with concurrent readers
//	cleanup   delete the perf-* namespaces
//
// Every subcommand that talks to Kubernetes takes -kubeconfig and
// -context and never reads or changes the current context.
package main

import (
	"fmt"
	"os"
	"sort"
)

type command struct {
	run  func(args []string) error
	help string
}

var commands = map[string]command{
	"populate": {runPopulate, "create the perf dataset"},
	"storm":    {runStorm, "update objects at a fixed rate"},
	"lag":      {runLag, "collection lag and loss from a storm CSV"},
	"bench":    {runBench, "API latency percentiles"},
	"burst":    {runBurst, "rapid updates of one object"},
	"recreate": {runRecreate, "delete + recreate the same name"},
	"compact":  {runCompact, "compaction with concurrent reads (store copy)"},
	"cleanup":  {runCleanup, "delete perf namespaces"},
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	c, ok := commands[os.Args[1]]
	if !ok {
		usage()
		os.Exit(2)
	}
	if err := c.run(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "chronosperf %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: chronosperf <command> [flags]")
	names := make([]string, 0, len(commands))
	for n := range commands {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(os.Stderr, "  %-9s %s\n", n, commands[n].help)
	}
}
