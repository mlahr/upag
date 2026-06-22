package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"upag/internal/app"
	"upag/internal/cli"
	"upag/internal/config"
	"upag/internal/daemon"
	"upag/internal/defaults"
	"upag/internal/logging"
	"upag/internal/storage"
)

var version = "dev"

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

	if args[0] == "--version" {
		fmt.Fprintln(os.Stdout, "upag", version)
		return nil
	}

	switch args[0] {
	case "run":
		return runDaemon(args[1:])
	case "start":
		return runStart(args[1:])
	case "stop":
		return runStop(args[1:])
	case "status":
		return runDaemonStatus(args[1:])
	case "restart":
		return runRestart(args[1:])
	case "config":
		return runConfig(args[1:])
	case "monitors":
		return runMonitors(args[1:])
	case "incidents":
		return runIncidents(args[1:])
	default:
		return usage()
	}
}

func runDaemon(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	dbPath := fs.String("db", defaults.StandaloneDBPath, "path to SQLite database")
	useSyslog := fs.Bool("syslog", false, "write daemon logs to syslog")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
		defaults.PathTarget{FlagName: "db", Value: dbPath, Default: func(d defaults.Paths) string { return d.DBPath }},
	); err != nil {
		return err
	}

	out := io.Writer(os.Stdout)
	errOut := io.Writer(os.Stderr)
	if *useSyslog {
		logger, err := logging.OpenSyslog("upag")
		if err != nil {
			return err
		}
		defer logger.Close()
		out = logger.InfoWriter()
		errOut = logger.ErrorWriter()
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

	runner, err := app.NewRunner(*configPath, cfg, store, out, errOut)
	if err != nil {
		return err
	}
	return runner.Run(ctx)
}

func runStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	dbPath := fs.String("db", defaults.StandaloneDBPath, "path to SQLite database")
	pidFile := fs.String("pid-file", defaults.StandalonePIDFile, "path to daemon PID file")
	logFile := fs.String("log-file", defaults.StandaloneLogFile, "path to daemon log file")
	useSyslog := fs.Bool("syslog", false, "write daemon logs to syslog")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
		defaults.PathTarget{FlagName: "db", Value: dbPath, Default: func(d defaults.Paths) string { return d.DBPath }},
		defaults.PathTarget{FlagName: "pid-file", Value: pidFile, Default: func(d defaults.Paths) string { return d.PIDFile }},
	); err != nil {
		return err
	}

	pid, err := daemon.Start(daemon.Options{
		ConfigPath: *configPath,
		DBPath:     *dbPath,
		PIDFile:    *pidFile,
		LogFile:    *logFile,
		Syslog:     *useSyslog,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "upag daemon started with PID %d\n", pid)
	return nil
}

func runStop(args []string) error {
	fs := flag.NewFlagSet("stop", flag.ContinueOnError)
	pidFile := fs.String("pid-file", defaults.StandalonePIDFile, "path to daemon PID file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "pid-file", Value: pidFile, Default: func(d defaults.Paths) string { return d.PIDFile }},
	); err != nil {
		return err
	}
	if err := daemon.Stop(*pidFile, 5*time.Second); err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, "upag daemon stopped")
	return nil
}

func runDaemonStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	pidFile := fs.String("pid-file", defaults.StandalonePIDFile, "path to daemon PID file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "pid-file", Value: pidFile, Default: func(d defaults.Paths) string { return d.PIDFile }},
	); err != nil {
		return err
	}

	status, err := daemon.Inspect(*pidFile)
	if err != nil {
		return err
	}
	if status.Running {
		fmt.Fprintf(os.Stdout, "upag daemon is running with PID %d using pid file %s\n", status.PID, status.PIDFile)
		return nil
	}
	if status.StaleFile {
		return fmt.Errorf("upag daemon is not running; pid file %s is stale for PID %d", status.PIDFile, status.PID)
	}
	return daemon.ErrNotRunning
}

func runRestart(args []string) error {
	fs := flag.NewFlagSet("restart", flag.ContinueOnError)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	dbPath := fs.String("db", defaults.StandaloneDBPath, "path to SQLite database")
	pidFile := fs.String("pid-file", defaults.StandalonePIDFile, "path to daemon PID file")
	logFile := fs.String("log-file", defaults.StandaloneLogFile, "path to daemon log file")
	useSyslog := fs.Bool("syslog", false, "write daemon logs to syslog")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
		defaults.PathTarget{FlagName: "db", Value: dbPath, Default: func(d defaults.Paths) string { return d.DBPath }},
		defaults.PathTarget{FlagName: "pid-file", Value: pidFile, Default: func(d defaults.Paths) string { return d.PIDFile }},
	); err != nil {
		return err
	}

	if err := daemon.Stop(*pidFile, 5*time.Second); err != nil && err != daemon.ErrNotRunning {
		return err
	}
	pid, err := daemon.Start(daemon.Options{
		ConfigPath: *configPath,
		DBPath:     *dbPath,
		PIDFile:    *pidFile,
		LogFile:    *logFile,
		Syslog:     *useSyslog,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "upag daemon restarted with PID %d\n", pid)
	return nil
}

func runConfig(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: upag config <reload> [flags]")
	}
	switch args[0] {
	case "reload":
		return runConfigReload(args[1:])
	default:
		return fmt.Errorf("usage: upag config <reload> [flags]")
	}
}

func runConfigReload(args []string) error {
	fs := flag.NewFlagSet("config reload", flag.ContinueOnError)
	pidFile := fs.String("pid-file", defaults.StandalonePIDFile, "path to daemon PID file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "pid-file", Value: pidFile, Default: func(d defaults.Paths) string { return d.PIDFile }},
	); err != nil {
		return err
	}
	pid, err := daemon.Reload(*pidFile)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "upag daemon reloaded configuration with PID %d\n", pid)
	return nil
}

func runMonitors(args []string) error {
	fs := flag.NewFlagSet("monitors", flag.ContinueOnError)
	dbPath := fs.String("db", defaults.StandaloneDBPath, "path to SQLite database")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "db", Value: dbPath, Default: func(d defaults.Paths) string { return d.DBPath }},
	); err != nil {
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
	dbPath := fs.String("db", defaults.StandaloneDBPath, "path to SQLite database")
	limit := fs.Int("limit", 50, "maximum number of incidents to print")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "db", Value: dbPath, Default: func(d defaults.Paths) string { return d.DBPath }},
	); err != nil {
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
	return fmt.Errorf("usage: upag [--version] <run|start|stop|status|restart|config|monitors|incidents> [flags]")
}
