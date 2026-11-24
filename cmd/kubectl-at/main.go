// Command kubectl-at is the Chronos kubectl plugin. With the binary on
// PATH, "kubectl at -20m get pods" shows pods as they were 20 minutes ago.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/krishhna24/chronos/internal/cli"
)

// version is set at build time:
//
//	go build -ldflags "-X main.version=v0.1.0" ./cmd/kubectl-at
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Run(ctx, os.Args[1:], cli.Options{Version: version})
	stop()
	os.Exit(code)
}
