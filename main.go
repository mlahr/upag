package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"os/user"
	"strconv"
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
	case "maintenance":
		return runMaintenance(args[1:])
	case "storage":
		return runStorage(args[1:])
	default:
		return usage()
	}
}

func runDaemon(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	useSyslog := fs.Bool("syslog", false, "write daemon logs to syslog")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
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

	store, err := storage.OpenBackend(context.Background(), cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runner, err := app.NewRunner(*configPath, cfg, store, out, errOut, version)
	if err != nil {
		return err
	}
	return runner.Run(ctx)
}

func runStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	pidFile := fs.String("pid-file", defaults.StandalonePIDFile, "path to daemon PID file")
	logFile := fs.String("log-file", defaults.StandaloneLogFile, "path to daemon log file")
	useSyslog := fs.Bool("syslog", false, "write daemon logs to syslog")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
		defaults.PathTarget{FlagName: "pid-file", Value: pidFile, Default: func(d defaults.Paths) string { return d.PIDFile }},
	); err != nil {
		return err
	}

	pid, err := daemon.Start(daemon.Options{
		ConfigPath: *configPath,
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
	pidFile := fs.String("pid-file", defaults.StandalonePIDFile, "path to daemon PID file")
	logFile := fs.String("log-file", defaults.StandaloneLogFile, "path to daemon log file")
	useSyslog := fs.Bool("syslog", false, "write daemon logs to syslog")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
		defaults.PathTarget{FlagName: "pid-file", Value: pidFile, Default: func(d defaults.Paths) string { return d.PIDFile }},
	); err != nil {
		return err
	}

	if err := daemon.Stop(*pidFile, 5*time.Second); err != nil && err != daemon.ErrNotRunning {
		return err
	}
	pid, err := daemon.Start(daemon.Options{
		ConfigPath: *configPath,
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
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
	); err != nil {
		return err
	}

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		return err
	}
	store, err := storage.OpenBackend(context.Background(), cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()

	rows, err := store.ListStates(context.Background())
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	windows, err := store.ListMaintenanceWindows(context.Background(), storage.MaintenanceWindowFilter{Now: now})
	if err != nil {
		return err
	}
	return cli.PrintStates(os.Stdout, rows, activeMaintenanceByMonitor(windows, now))
}

func runIncidents(args []string) error {
	fs := flag.NewFlagSet("incidents", flag.ContinueOnError)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	limit := fs.Int("limit", 50, "maximum number of incidents to print")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
	); err != nil {
		return err
	}

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		return err
	}
	store, err := storage.OpenBackend(context.Background(), cfg.Storage)
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

func runMaintenance(args []string) error {
	if len(args) == 0 {
		return maintenanceUsage()
	}
	switch args[0] {
	case "add":
		return runMaintenanceAdd(args[1:])
	case "cancel":
		return runMaintenanceCancel(args[1:])
	case "list":
		return runMaintenanceList(args[1:])
	default:
		return maintenanceUsage()
	}
}

func runStorage(args []string) error {
	if len(args) == 0 {
		return storageUsage()
	}
	switch args[0] {
	case "migrate":
		return runStorageMigrate(args[1:])
	default:
		return storageUsage()
	}
}

func runStorageMigrate(args []string) error {
	fs := flag.NewFlagSet("storage migrate", flag.ContinueOnError)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	fromSQLite := fs.String("from-sqlite", "", "source SQLite database path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
	); err != nil {
		return err
	}
	if *fromSQLite == "" {
		return fmt.Errorf("storage migrate requires --from-sqlite")
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		return err
	}
	if cfg.Storage.Backend != "postgres" {
		return fmt.Errorf("storage migrate target config must use storage.backend: postgres")
	}
	if err := storage.MigrateSQLiteToPostgres(context.Background(), *fromSQLite, cfg.Storage.Postgres.DSN); err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, "SQLite data migrated to PostgreSQL storage")
	return nil
}

func runMaintenanceAdd(args []string) error {
	fs := flag.NewFlagSet("maintenance add", flag.ContinueOnError)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	monitorID := fs.String("monitor", "", "monitor ID")
	startRaw := fs.String("start", "", "maintenance start time in RFC3339 format")
	endRaw := fs.String("end", "", "maintenance end time in RFC3339 format")
	reason := fs.String("reason", "", "maintenance reason")
	actor := fs.String("by", "", "operator identity for audit records")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
	); err != nil {
		return err
	}
	if *monitorID == "" {
		return fmt.Errorf("maintenance add requires --monitor")
	}
	start, err := parseCLITime(*startRaw, "--start")
	if err != nil {
		return err
	}
	end, err := parseCLITime(*endRaw, "--end")
	if err != nil {
		return err
	}
	by, err := auditActor(*actor)
	if err != nil {
		return err
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		return err
	}
	store, err := storage.OpenBackend(context.Background(), cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()
	id, err := store.AddMaintenanceWindow(context.Background(), storage.MaintenanceWindow{
		MonitorID: *monitorID,
		StartsAt:  start,
		EndsAt:    end,
		Reason:    *reason,
		CreatedBy: by,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "maintenance window %d scheduled for monitor %s\n", id, *monitorID)
	return nil
}

func runMaintenanceCancel(args []string) error {
	fs := flag.NewFlagSet("maintenance cancel", flag.ContinueOnError)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	idRaw := fs.String("id", "", "maintenance window ID")
	reason := fs.String("reason", "", "cancellation reason")
	actor := fs.String("by", "", "operator identity for audit records")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
	); err != nil {
		return err
	}
	if *idRaw == "" {
		return fmt.Errorf("maintenance cancel requires --id")
	}
	id, err := strconv.ParseInt(*idRaw, 10, 64)
	if err != nil || id <= 0 {
		return fmt.Errorf("maintenance cancel --id must be a positive integer")
	}
	by, err := auditActor(*actor)
	if err != nil {
		return err
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		return err
	}
	store, err := storage.OpenBackend(context.Background(), cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.CancelMaintenanceWindow(context.Background(), id, time.Now().UTC(), by, *reason); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "maintenance window %d cancelled\n", id)
	return nil
}

func runMaintenanceList(args []string) error {
	fs := flag.NewFlagSet("maintenance list", flag.ContinueOnError)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	monitorID := fs.String("monitor", "", "monitor ID")
	includeAll := fs.Bool("all", false, "include cancelled and ended maintenance windows")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := defaults.ApplyPaths(fs,
		defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
	); err != nil {
		return err
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		return err
	}
	store, err := storage.OpenBackend(context.Background(), cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()
	now := time.Now().UTC()
	filter := storage.MaintenanceWindowFilter{MonitorID: *monitorID, IncludeAll: *includeAll}
	if !*includeAll {
		filter.Now = now
	}
	windows, err := store.ListMaintenanceWindows(context.Background(), filter)
	if err != nil {
		return err
	}
	return cli.PrintMaintenanceWindows(os.Stdout, windows, now)
}

func activeMaintenanceByMonitor(windows []storage.MaintenanceWindow, now time.Time) map[string]storage.MaintenanceWindow {
	active := map[string]storage.MaintenanceWindow{}
	for _, window := range windows {
		if !window.CancelledAt.IsZero() || now.Before(window.StartsAt) || !now.Before(window.EndsAt) {
			continue
		}
		active[window.MonitorID] = window
	}
	return active
}

func parseCLITime(raw string, flagName string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, fmt.Errorf("maintenance add requires %s", flagName)
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be RFC3339 timestamp: %w", flagName, err)
	}
	return parsed.UTC(), nil
}

func auditActor(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	current, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("resolve current user for audit actor: %w", err)
	}
	if current.Username == "" {
		return "", fmt.Errorf("current user name is empty; pass --by")
	}
	return current.Username, nil
}

func maintenanceUsage() error {
	return fmt.Errorf("usage: upag maintenance <add|cancel|list> [flags]")
}

func storageUsage() error {
	return fmt.Errorf("usage: upag storage <migrate> [flags]")
}

func usage() error {
	return fmt.Errorf("usage: upag [--version] <run|start|stop|status|restart|config|monitors|incidents|maintenance|storage> [flags]")
}
