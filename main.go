package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"upag/internal/app"
	"upag/internal/cli"
	"upag/internal/config"
	"upag/internal/storage"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usage()
	}

	switch args[0] {
	case "run":
		return runDaemon(args[1:])
	case "status":
		return runStatus(args[1:])
	case "incidents":
		return runIncidents(args[1:])
	default:
		return usage()
	}
}

func runDaemon(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	configPath := fs.String("config", "./config.yaml", "path to YAML configuration")
	dbPath := fs.String("db", "./upag.sqlite", "path to SQLite database")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		return err
	}

	store, err := storage.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runner, err := app.NewRunner(*configPath, cfg, store, os.Stdout, os.Stderr)
	if err != nil {
		return err
	}
	return runner.Run(ctx)
}

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	dbPath := fs.String("db", "./upag.sqlite", "path to SQLite database")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := storage.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	rows, err := store.ListStates(context.Background())
	if err != nil {
		return err
	}
	return cli.PrintStates(os.Stdout, rows)
}

func runIncidents(args []string) error {
	fs := flag.NewFlagSet("incidents", flag.ContinueOnError)
	dbPath := fs.String("db", "./upag.sqlite", "path to SQLite database")
	limit := fs.Int("limit", 50, "maximum number of incidents to print")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := storage.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	rows, err := store.ListIncidents(context.Background(), *limit)
	if err != nil {
		return err
	}
	return cli.PrintIncidents(os.Stdout, rows)
}

func usage() error {
	return fmt.Errorf("usage: upag <run|status|incidents> [flags]")
}
