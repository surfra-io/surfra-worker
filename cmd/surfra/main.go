package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"surfra.io/worker/internal/job"
	"surfra.io/worker/internal/supervisor"
)

var version = "0.2.4"

func main() {
	// Clouds that take a command append "surfra worker" after the image entrypoint.
	if len(os.Args) > 2 && os.Args[1] == "surfra" {
		os.Args = append(os.Args[:1], os.Args[2:]...)
	}
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version", "--version", "-v":
		fmt.Println(version)
	case "worker":
		runWorker()
	case "job":
		if len(os.Args) < 3 || os.Args[2] != "agent" {
			usage()
			os.Exit(2)
		}
		runJobAgent()
	default:
		usage()
		os.Exit(2)
	}
}

func runWorker() {
	cfg, err := supervisor.FromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := supervisor.Run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runJobAgent() {
	cfg, err := job.FromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	// The agent handles SIGTERM and SIGINT itself: it stops the command, then reports how it ended.
	os.Exit(job.Run(context.Background(), cfg))
}

func usage() {
	fmt.Fprintf(os.Stderr, "surfra %s\nusage: surfra worker\n       surfra job agent\n       surfra version\n", version)
}
