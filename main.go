package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/signal"
	"os/user"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/term"

	"upag/internal/app"
	"upag/internal/checker"
	"upag/internal/cli"
	"upag/internal/config"
	"upag/internal/controlapi"
	"upag/internal/daemon"
	"upag/internal/defaults"
	"upag/internal/logging"
	"upag/internal/storage"
)

var version = "dev"
var storageDSNReadPassword = readStorageDSNPassword

func main() {
	args := os.Args[1:]
	if err := run(args); err != nil {
		if outputErr := printRunError(os.Stderr, args, err); outputErr != nil {
			fmt.Fprintln(os.Stderr, outputErr)
		}
		os.Exit(1)
	}
}

func printRunError(w io.Writer, args []string, err error) error {
	if jsonOutputRequested(args) {
		return cli.PrintErrorJSON(w, err)
	}
	_, outputErr := fmt.Fprintln(w, err)
	return outputErr
}

func run(args []string) error {
	if len(args) == 0 {
		return usage()
	}

	if args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		if len(args) != 1 {
			return fmt.Errorf("%s does not accept arguments", args[0])
		}
		return printHelp(os.Stdout)
	}
	if args[0] == "--version" {
		if len(args) != 1 {
			return fmt.Errorf("--version does not accept arguments")
		}
		fmt.Fprintln(os.Stdout, "upag", version)
		return nil
	}

	global, commandArgs, err := parseGlobalOptions(args)
	if err != nil {
		return err
	}
	if len(commandArgs) == 0 {
		return usage()
	}
	if commandArgs[0] == "help" || commandArgs[0] == "--help" || commandArgs[0] == "-h" {
		if global.JSON {
			return fmt.Errorf("--json cannot be used with help")
		}
		if len(commandArgs) != 1 {
			return fmt.Errorf("%s does not accept arguments", commandArgs[0])
		}
		return printHelp(os.Stdout)
	}
	if commandArgs[0] == "--version" {
		if len(commandArgs) != 1 {
			return fmt.Errorf("--version does not accept arguments")
		}
		if global.JSON {
			return cli.PrintJSON(os.Stdout, versionResult{Version: version})
		}
		fmt.Fprintln(os.Stdout, "upag", version)
		return nil
	}
	if !knownCommand(commandArgs[0]) {
		return usage()
	}
	var remote *controlapi.Client
	if global.Remote != "" {
		remote, err = controlapi.NewClient(global.Remote, global.Token, global.Timeout)
		if err != nil {
			return err
		}
	}
	args = commandArgs
	if global.JSON && args[0] == "run" {
		return fmt.Errorf("run does not support --json")
	}
	if remote != nil && !remoteCapableCommand(args[0]) {
		return fmt.Errorf("%s cannot run remotely; pass --local to run it on this host", args[0])
	}

	switch args[0] {
	case "run":
		return runDaemon(args[1:])
	case "start":
		return runStart(args[1:], global.JSON)
	case "stop":
		return runStop(args[1:], global.JSON)
	case "status":
		return runDaemonStatus(args[1:], remote, global.JSON)
	case "restart":
		return runRestart(args[1:], global.JSON)
	case "config":
		return runConfig(args[1:], global.JSON)
	case "check":
		return runCheck(args[1:], remote, global.JSON)
	case "monitors":
		return runMonitors(args[1:], remote, global.JSON)
	case "uptime":
		return runUptime(args[1:], remote, global.JSON)
	case "incidents":
		return runIncidents(args[1:], remote, global.JSON)
	case "intervals":
		return runIntervals(args[1:], remote, global.JSON)
	case "failures":
		return runFailures(args[1:], remote, global.JSON)
	case "maintenance":
		return runMaintenance(args[1:], remote, global.JSON)
	case "storage":
		return runStorage(args[1:], global.JSON)
	default:
		return usage()
	}
}

type globalOptions struct {
	Remote  string
	Token   string
	Timeout time.Duration
	JSON    bool
}

func parseGlobalOptions(args []string) (globalOptions, []string, error) {
	options := globalOptions{Timeout: time.Minute}
	remoteSet := false
	tokenSet := false
	timeoutSet := false
	local := false
	index := 0
	readValue := func(name string) (string, error) {
		index++
		if index >= len(args) {
			return "", fmt.Errorf("%s requires a value", name)
		}
		return args[index], nil
	}
	for index < len(args) {
		arg := args[index]
		if !strings.HasPrefix(arg, "--") || arg == "--" {
			break
		}
		if arg == "--version" || arg == "--help" {
			break
		}
		var value string
		var err error
		switch {
		case arg == "--remote":
			value, err = readValue("--remote")
			if err == nil {
				options.Remote, remoteSet = value, true
			}
		case strings.HasPrefix(arg, "--remote="):
			options.Remote, remoteSet = strings.TrimPrefix(arg, "--remote="), true
		case arg == "--token":
			value, err = readValue("--token")
			if err == nil {
				options.Token, tokenSet = value, true
			}
		case strings.HasPrefix(arg, "--token="):
			options.Token, tokenSet = strings.TrimPrefix(arg, "--token="), true
		case arg == "--remote-timeout":
			value, err = readValue("--remote-timeout")
			if err == nil {
				options.Timeout, err = time.ParseDuration(value)
				timeoutSet = true
			}
		case strings.HasPrefix(arg, "--remote-timeout="):
			options.Timeout, err = time.ParseDuration(strings.TrimPrefix(arg, "--remote-timeout="))
			timeoutSet = true
		case arg == "--local":
			local = true
		case arg == "--json":
			options.JSON = true
		default:
			return globalOptions{}, nil, fmt.Errorf("unknown global option %s; global options must appear before the command", arg)
		}
		if err != nil {
			return globalOptions{}, nil, err
		}
		index++
	}
	if local && (remoteSet || tokenSet || timeoutSet) {
		return globalOptions{}, nil, fmt.Errorf("--local cannot be combined with remote options")
	}
	if local {
		return options, args[index:], nil
	}
	if remoteSet && options.Remote == "" {
		return globalOptions{}, nil, fmt.Errorf("--remote requires a non-empty URL")
	}
	if !remoteSet {
		options.Remote = os.Getenv("UPAG_REMOTE")
	}
	if !tokenSet {
		options.Token = os.Getenv("UPAG_TOKEN")
	}
	if !timeoutSet {
		if raw := os.Getenv("UPAG_REMOTE_TIMEOUT"); raw != "" {
			parsed, err := time.ParseDuration(raw)
			if err != nil {
				return globalOptions{}, nil, fmt.Errorf("UPAG_REMOTE_TIMEOUT: %w", err)
			}
			options.Timeout = parsed
		}
	}
	if options.Remote == "" && (options.Token != "" || timeoutSet || os.Getenv("UPAG_REMOTE_TIMEOUT") != "") {
		return globalOptions{}, nil, fmt.Errorf("remote options require --remote or UPAG_REMOTE")
	}
	return options, args[index:], nil
}

func jsonOutputRequested(args []string) bool {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if arg == "--json" {
			return true
		}
		switch {
		case arg == "--local", strings.HasPrefix(arg, "--remote="), strings.HasPrefix(arg, "--token="), strings.HasPrefix(arg, "--remote-timeout="):
			continue
		case arg == "--remote", arg == "--token", arg == "--remote-timeout":
			index++
			continue
		}
		if !strings.HasPrefix(arg, "--") || arg == "--" || arg == "--version" || arg == "--help" {
			return false
		}
		return false
	}
	return false
}

func newCommandFlagSet(name string, jsonOutput bool) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	if jsonOutput {
		fs.SetOutput(io.Discard)
	}
	return fs
}

type versionResult struct {
	Version string `json:"version"`
}

type actionResult struct {
	Status string `json:"status"`
	PID    int    `json:"pid,omitempty"`
}

type localStatusResult struct {
	Status  string `json:"status"`
	PID     int    `json:"pid"`
	PIDFile string `json:"pid_file"`
}

type remoteStatusResult struct {
	Status    string    `json:"status"`
	Version   string    `json:"version"`
	StartedAt time.Time `json:"started_at"`
	RemoteURL string    `json:"remote_url"`
}

type storageDSNResult struct {
	Backend string `json:"backend"`
	DSN     string `json:"dsn"`
}

type storageMigrationResult struct {
	Status   string `json:"status"`
	Source   string `json:"source"`
	Target   string `json:"target"`
	TenantID string `json:"tenant_id"`
}

func remoteCapableCommand(command string) bool {
	switch command {
	case "status", "check", "monitors", "uptime", "incidents", "intervals", "failures", "maintenance":
		return true
	default:
		return false
	}
}

func knownCommand(command string) bool {
	switch command {
	case "run", "start", "stop", "status", "restart", "config", "check", "monitors", "uptime", "incidents", "intervals", "failures", "maintenance", "storage":
		return true
	default:
		return false
	}
}

func runCheck(args []string, remote *controlapi.Client, jsonOutput bool) error {
	fs := newCommandFlagSet("check", jsonOutput)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	monitorID := fs.String("monitor", "", "monitor ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("check does not accept positional arguments")
	}
	if *monitorID == "" {
		return fmt.Errorf("check requires --monitor")
	}
	if remote != nil {
		if defaults.FlagWasSet(fs, "config") {
			return fmt.Errorf("check --config cannot be used with --remote")
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		response, err := remote.Check(ctx, *monitorID)
		if err != nil {
			return err
		}
		diagnostic := diagnosticFromRemote(response)
		if jsonOutput {
			err = cli.PrintDiagnosticJSON(os.Stdout, diagnostic)
		} else {
			err = cli.PrintDiagnosticText(os.Stdout, diagnostic)
		}
		if err != nil {
			return err
		}
		if !diagnostic.OK {
			return fmt.Errorf("diagnostic check failed")
		}
		return nil
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
	var monitor config.MonitorConfig
	found := false
	for _, candidate := range cfg.Monitors {
		if candidate.ID == *monitorID {
			monitor = candidate
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("monitor %q is not configured", *monitorID)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result := checker.Check(ctx, monitor)
	diagnostic := cli.DiagnosticResult{
		MonitorID:          monitor.ID,
		Name:               monitor.Name,
		ConfiguredURL:      monitor.URL,
		FinalURL:           result.FinalURL,
		OK:                 result.OK,
		ExpectedStatusCode: monitor.ExpectedStatusCode,
		ObservedStatusCode: result.ObservedStatusCode,
		RedirectsFollowed:  result.RedirectsFollowed,
		LatencyMS:          result.Latency.Milliseconds(),
		ResponseTimeMS:     result.ResponseTime.Milliseconds(),
		CheckedAt:          result.CheckedAt.UTC(),
		Error:              result.Error,
	}
	if jsonOutput {
		err = cli.PrintDiagnosticJSON(os.Stdout, diagnostic)
	} else {
		err = cli.PrintDiagnosticText(os.Stdout, diagnostic)
	}
	if err != nil {
		return err
	}
	if !result.OK {
		return fmt.Errorf("diagnostic check failed")
	}
	return nil
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

func runStart(args []string, jsonOutput bool) error {
	fs := newCommandFlagSet("start", jsonOutput)
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
	if jsonOutput {
		return cli.PrintJSON(os.Stdout, actionResult{Status: "started", PID: pid})
	}
	fmt.Fprintf(os.Stdout, "upag daemon started with PID %d\n", pid)
	return nil
}

func runStop(args []string, jsonOutput bool) error {
	fs := newCommandFlagSet("stop", jsonOutput)
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
	if jsonOutput {
		return cli.PrintJSON(os.Stdout, actionResult{Status: "stopped"})
	}
	fmt.Fprintln(os.Stdout, "upag daemon stopped")
	return nil
}

func runDaemonStatus(args []string, remote *controlapi.Client, jsonOutput bool) error {
	fs := newCommandFlagSet("status", jsonOutput)
	pidFile := fs.String("pid-file", defaults.StandalonePIDFile, "path to daemon PID file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if remote != nil {
		if defaults.FlagWasSet(fs, "pid-file") {
			return fmt.Errorf("status --pid-file cannot be used with --remote")
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		response, err := remote.Status(ctx)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cli.PrintJSON(os.Stdout, remoteStatusResult{
				Status:    response.Status,
				Version:   response.Version,
				StartedAt: response.StartedAt,
				RemoteURL: remote.BaseURL(),
			})
		}
		fmt.Fprintf(os.Stdout, "upag remote daemon at %s is reachable (version %s, started %s)\n", remote.BaseURL(), response.Version, response.StartedAt.UTC().Format(time.RFC3339Nano))
		return nil
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
		if jsonOutput {
			return cli.PrintJSON(os.Stdout, localStatusResult{Status: "running", PID: status.PID, PIDFile: status.PIDFile})
		}
		fmt.Fprintf(os.Stdout, "upag daemon is running with PID %d using pid file %s\n", status.PID, status.PIDFile)
		return nil
	}
	if status.StaleFile {
		return fmt.Errorf("upag daemon is not running; pid file %s is stale for PID %d", status.PIDFile, status.PID)
	}
	return daemon.ErrNotRunning
}

func runRestart(args []string, jsonOutput bool) error {
	fs := newCommandFlagSet("restart", jsonOutput)
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
	if jsonOutput {
		return cli.PrintJSON(os.Stdout, actionResult{Status: "restarted", PID: pid})
	}
	fmt.Fprintf(os.Stdout, "upag daemon restarted with PID %d\n", pid)
	return nil
}

func runConfig(args []string, jsonOutput bool) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: upag config <reload> [flags]")
	}
	switch args[0] {
	case "reload":
		return runConfigReload(args[1:], jsonOutput)
	default:
		return fmt.Errorf("usage: upag config <reload> [flags]")
	}
}

func runConfigReload(args []string, jsonOutput bool) error {
	fs := newCommandFlagSet("config reload", jsonOutput)
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
	if jsonOutput {
		return cli.PrintJSON(os.Stdout, actionResult{Status: "reloaded", PID: pid})
	}
	fmt.Fprintf(os.Stdout, "upag daemon reloaded configuration with PID %d\n", pid)
	return nil
}

func runMonitors(args []string, remote *controlapi.Client, jsonOutput bool) error {
	fs := newCommandFlagSet("monitors", jsonOutput)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if remote != nil {
		if defaults.FlagWasSet(fs, "config") {
			return fmt.Errorf("monitors --config cannot be used with --remote")
		}
		response, err := remote.Monitors(context.Background())
		if err != nil {
			return err
		}
		if jsonOutput {
			return cli.PrintJSON(os.Stdout, response)
		}
		states := make([]storage.MonitorState, 0, len(response.Monitors))
		for _, monitor := range response.Monitors {
			states = append(states, monitor.Storage())
		}
		active := make(map[string]storage.MaintenanceWindow, len(response.ActiveMaintenance))
		for _, window := range response.ActiveMaintenance {
			stored := window.Storage()
			active[stored.MonitorID] = stored
		}
		return cli.PrintStates(os.Stdout, states, active)
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
	ctx := storage.WithTenant(context.Background(), cfg.TenantID)
	store, err := storage.OpenBackend(ctx, cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()

	rows, err := store.ListStates(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	windows, err := store.ListMaintenanceWindows(ctx, storage.MaintenanceWindowFilter{Now: now})
	if err != nil {
		return err
	}
	if jsonOutput {
		activeRows := make([]storage.MaintenanceWindow, 0)
		for _, window := range windows {
			if window.CancelledAt.IsZero() && !now.Before(window.StartsAt) && now.Before(window.EndsAt) {
				activeRows = append(activeRows, window)
			}
		}
		return cli.PrintJSON(os.Stdout, controlapi.MonitorsResponse{
			GeneratedAt:       now,
			Monitors:          controlapi.MonitorsFromStorage(rows),
			ActiveMaintenance: controlapi.MaintenanceFromStorage(activeRows),
		})
	}
	return cli.PrintStates(os.Stdout, rows, activeMaintenanceByMonitor(windows, now))
}

func runUptime(args []string, remote *controlapi.Client, jsonOutput bool) error {
	fs := newCommandFlagSet("uptime", jsonOutput)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("uptime does not accept positional arguments")
	}

	var response controlapi.UptimeResponse
	if remote != nil {
		if defaults.FlagWasSet(fs, "config") {
			return fmt.Errorf("uptime --config cannot be used with --remote")
		}
		var err error
		response, err = remote.Uptime(context.Background())
		if err != nil {
			return err
		}
	} else {
		if err := defaults.ApplyPaths(fs,
			defaults.PathTarget{FlagName: "config", Value: configPath, Default: func(d defaults.Paths) string { return d.ConfigPath }},
		); err != nil {
			return err
		}
		cfg, err := config.LoadFile(*configPath)
		if err != nil {
			return err
		}
		ctx := storage.WithTenant(context.Background(), cfg.TenantID)
		store, err := storage.OpenBackend(ctx, cfg.Storage)
		if err != nil {
			return err
		}
		defer store.Close()
		if err := store.EnsureStatusIntervalsBackfilled(ctx, configuredFailureThresholds(cfg)); err != nil {
			return err
		}
		states, err := store.ListStates(ctx)
		if err != nil {
			return err
		}
		starts, err := store.ListUptimeStreakStarts(ctx)
		if err != nil {
			return err
		}
		response = controlapi.UptimeResponseFromStorage(states, starts, time.Now().UTC())
	}
	sortUptimeMonitors(response.Monitors)

	if jsonOutput {
		return cli.PrintJSON(os.Stdout, response)
	}
	rows := make([]storage.MonitorUptime, 0, len(response.Monitors))
	for _, monitor := range response.Monitors {
		rows = append(rows, monitor.Storage())
	}
	return cli.PrintUptimeAt(os.Stdout, rows, response.GeneratedAt)
}

func sortUptimeMonitors(monitors []controlapi.UptimeMonitor) {
	sort.Slice(monitors, func(i, j int) bool {
		left := monitors[i].FailureFreeSeconds
		right := monitors[j].FailureFreeSeconds
		switch {
		case left == nil && right == nil:
			return monitors[i].MonitorID < monitors[j].MonitorID
		case left == nil:
			return false
		case right == nil:
			return true
		case *left == *right:
			return monitors[i].MonitorID < monitors[j].MonitorID
		default:
			return *left < *right
		}
	})
}

func runIncidents(args []string, remote *controlapi.Client, jsonOutput bool) error {
	fs := newCommandFlagSet("incidents", jsonOutput)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	limit := fs.Int("limit", 50, "maximum number of incidents to print")
	sinceRaw := fs.String("since", "", "only print incidents since an RFC3339 timestamp or positive duration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	since, err := parseCLISince(*sinceRaw, "--since", time.Now().UTC())
	if err != nil {
		return err
	}
	if remote != nil {
		if defaults.FlagWasSet(fs, "config") {
			return fmt.Errorf("incidents --config cannot be used with --remote")
		}
		response, err := remote.Incidents(context.Background(), *limit, since)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cli.PrintJSON(os.Stdout, response)
		}
		rows := make([]storage.Incident, 0, len(response.Incidents))
		for _, incident := range response.Incidents {
			rows = append(rows, incident.Storage())
		}
		return cli.PrintIncidents(os.Stdout, rows)
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
	ctx := storage.WithTenant(context.Background(), cfg.TenantID)
	store, err := storage.OpenBackend(ctx, cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()

	rows, err := store.ListIncidents(ctx, storage.IncidentFilter{
		Limit: *limit,
		Since: since,
	})
	if err != nil {
		return err
	}
	if jsonOutput {
		return cli.PrintJSON(os.Stdout, controlapi.IncidentsResponse{Incidents: controlapi.IncidentsFromStorage(rows)})
	}
	return cli.PrintIncidents(os.Stdout, rows)
}

func runIntervals(args []string, remote *controlapi.Client, jsonOutput bool) error {
	fs := newCommandFlagSet("intervals", jsonOutput)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	monitorID := fs.String("monitor", "", "monitor ID")
	limit := fs.Int("limit", 50, "maximum number of intervals to print")
	sinceRaw := fs.String("since", "", "only print intervals since an RFC3339 timestamp or positive duration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *limit <= 0 {
		return fmt.Errorf("intervals --limit must be positive")
	}
	now := time.Now().UTC()
	since, err := parseCLISince(*sinceRaw, "--since", now)
	if err != nil {
		return err
	}
	if remote != nil {
		if defaults.FlagWasSet(fs, "config") {
			return fmt.Errorf("intervals --config cannot be used with --remote")
		}
		response, err := remote.Intervals(context.Background(), *monitorID, *limit, since)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cli.PrintJSON(os.Stdout, response)
		}
		rows := make([]storage.StatusInterval, 0, len(response.Intervals))
		for _, interval := range response.Intervals {
			rows = append(rows, interval.Storage())
		}
		return cli.PrintStatusIntervalsAt(os.Stdout, rows, response.GeneratedAt)
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
	ctx := storage.WithTenant(context.Background(), cfg.TenantID)
	store, err := storage.OpenBackend(ctx, cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.EnsureStatusIntervalsBackfilled(ctx, configuredFailureThresholds(cfg)); err != nil {
		return err
	}

	intervals, err := store.ListStatusIntervals(ctx, storage.StatusIntervalFilter{
		MonitorID: *monitorID,
		Limit:     *limit,
		Since:     since,
		Now:       now,
	})
	if err != nil {
		return err
	}
	if jsonOutput {
		return cli.PrintJSON(os.Stdout, controlapi.IntervalsResponse{GeneratedAt: now, Intervals: controlapi.IntervalsFromStorage(intervals)})
	}
	return cli.PrintStatusIntervals(os.Stdout, intervals)
}

func configuredFailureThresholds(cfg config.Config) storage.FailureThresholds {
	thresholds := storage.FailureThresholds{
		Default:  cfg.Defaults.FailureThreshold,
		Monitors: map[string]int{},
	}
	for _, monitor := range cfg.Monitors {
		if monitor.FailureThreshold > 0 {
			thresholds.Monitors[monitor.ID] = monitor.FailureThreshold
		}
	}
	return thresholds
}

func diagnosticFromRemote(result controlapi.Diagnostic) cli.DiagnosticResult {
	return cli.DiagnosticResult{
		MonitorID: result.MonitorID, Name: result.Name, ConfiguredURL: result.ConfiguredURL,
		FinalURL: result.FinalURL, OK: result.OK, ExpectedStatusCode: result.ExpectedStatusCode,
		ObservedStatusCode: result.ObservedStatusCode, RedirectsFollowed: result.RedirectsFollowed,
		LatencyMS: result.LatencyMS, ResponseTimeMS: result.ResponseTimeMS,
		CheckedAt: result.CheckedAt, Error: result.Error,
	}
}

func runFailures(args []string, remote *controlapi.Client, jsonOutput bool) error {
	fs := newCommandFlagSet("failures", jsonOutput)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	limit := fs.Int("limit", 50, "maximum number of failures per section")
	sinceRaw := fs.String("since", "", "only print failures since an RFC3339 timestamp or positive duration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	since, err := parseCLISince(*sinceRaw, "--since", time.Now().UTC())
	if err != nil {
		return err
	}
	if remote != nil {
		if defaults.FlagWasSet(fs, "config") {
			return fmt.Errorf("failures --config cannot be used with --remote")
		}
		response, err := remote.Failures(context.Background(), *limit, since)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cli.PrintJSON(os.Stdout, response)
		}
		probes := make([]storage.ProbeResult, 0, len(response.FailedProbes))
		for _, probe := range response.FailedProbes {
			probes = append(probes, probe.Storage())
		}
		events := make([]storage.ObserverSentinelResult, 0, len(response.SentinelEvents))
		for _, event := range response.SentinelEvents {
			events = append(events, event.Storage())
		}
		return cli.PrintFailures(os.Stdout, probes, response.Observer.Storage(), response.ObserverKnown, events)
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
	ctx := storage.WithTenant(context.Background(), cfg.TenantID)
	store, err := storage.OpenBackend(ctx, cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()

	failedProbes, err := store.ListFailedProbeResults(ctx, storage.ProbeResultFilter{
		Limit: *limit,
		Since: since,
	})
	if err != nil {
		return err
	}
	observerState, observerKnown, err := store.GetObserverState(ctx)
	if err != nil {
		return err
	}
	sentinelEvents, err := store.ListObserverSentinelEvents(ctx, storage.ObserverSentinelEventFilter{
		Limit: *limit,
		Since: since,
	})
	if err != nil {
		return err
	}
	if jsonOutput {
		return cli.PrintJSON(os.Stdout, controlapi.FailuresResponse{
			FailedProbes:   controlapi.ProbeFailuresFromStorage(failedProbes),
			Observer:       controlapi.ObserverStateFromStorage(observerState),
			ObserverKnown:  observerKnown,
			SentinelEvents: controlapi.SentinelEventsFromStorage(sentinelEvents),
		})
	}
	return cli.PrintFailures(os.Stdout, failedProbes, observerState, observerKnown, sentinelEvents)
}

func runMaintenance(args []string, remote *controlapi.Client, jsonOutput bool) error {
	if len(args) == 0 {
		return maintenanceUsage()
	}
	switch args[0] {
	case "add":
		return runMaintenanceAdd(args[1:], remote, jsonOutput)
	case "cancel":
		return runMaintenanceCancel(args[1:], remote, jsonOutput)
	case "list":
		return runMaintenanceList(args[1:], remote, jsonOutput)
	default:
		return maintenanceUsage()
	}
}

func runStorage(args []string, jsonOutput bool) error {
	if len(args) == 0 {
		return storageUsage()
	}
	switch args[0] {
	case "migrate":
		return runStorageMigrate(args[1:], jsonOutput)
	case "dsn":
		return runStorageDSN(args[1:], jsonOutput)
	default:
		return storageUsage()
	}
}

type postgresDSNOptions struct {
	Host     string
	User     string
	Password string
	Database string
	Port     int
	SSLMode  string
}

func buildPostgresDSN(opts postgresDSNOptions) (string, error) {
	if err := validatePostgresDSNOptions(opts); err != nil {
		return "", err
	}
	values := url.Values{}
	values.Set("sslmode", strings.TrimSpace(opts.SSLMode))
	dsn := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(strings.TrimSpace(opts.User), opts.Password),
		Host:     net.JoinHostPort(strings.TrimSpace(opts.Host), strconv.Itoa(opts.Port)),
		Path:     "/" + strings.TrimSpace(opts.Database),
		RawQuery: values.Encode(),
	}
	return dsn.String(), nil
}

func validatePostgresDSNOptions(opts postgresDSNOptions) error {
	host := strings.TrimSpace(opts.Host)
	if host == "" {
		return fmt.Errorf("storage dsn requires --host")
	}
	user := strings.TrimSpace(opts.User)
	if user == "" {
		return fmt.Errorf("storage dsn requires --user")
	}
	database := strings.TrimSpace(opts.Database)
	if database == "" {
		return fmt.Errorf("storage dsn --database is required")
	}
	if opts.Port <= 0 || opts.Port > 65535 {
		return fmt.Errorf("storage dsn --port must be between 1 and 65535")
	}
	sslMode := strings.TrimSpace(opts.SSLMode)
	if sslMode == "" {
		return fmt.Errorf("storage dsn --sslmode is required")
	}
	return nil
}

func runStorageDSN(args []string, jsonOutput bool) error {
	fs := newCommandFlagSet("storage dsn", jsonOutput)
	host := fs.String("host", "", "PostgreSQL host, for example db.xxxxx.supabase.co")
	user := fs.String("user", "", "PostgreSQL username")
	database := fs.String("database", "postgres", "PostgreSQL database name")
	port := fs.Int("port", 5432, "PostgreSQL port")
	sslMode := fs.String("sslmode", "require", "PostgreSQL sslmode query parameter")
	timeout := fs.Duration("timeout", 10*time.Second, "connectivity test timeout")
	noTest := fs.Bool("no-test", false, "print the DSN without testing connectivity")
	format := fs.String("format", "yaml", "output format: yaml or dsn")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if jsonOutput && defaults.FlagWasSet(fs, "format") {
		return fmt.Errorf("storage dsn --format cannot be combined with --json")
	}
	if *format != "yaml" && *format != "dsn" {
		return fmt.Errorf("storage dsn --format must be one of: yaml, dsn")
	}
	if *timeout <= 0 {
		return fmt.Errorf("storage dsn --timeout must be positive")
	}

	opts := postgresDSNOptions{
		Host:     *host,
		User:     *user,
		Database: *database,
		Port:     *port,
		SSLMode:  *sslMode,
	}
	if err := validatePostgresDSNOptions(opts); err != nil {
		return err
	}
	password, err := storageDSNReadPassword()
	if err != nil {
		return err
	}
	opts.Password = password
	dsn, err := buildPostgresDSN(opts)
	if err != nil {
		return err
	}

	if !*noTest {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		fmt.Fprintln(os.Stderr, "Testing PostgreSQL connectivity with SELECT 1...")
		if err := testPostgresDSN(ctx, dsn); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "PostgreSQL connectivity OK")
	}

	if jsonOutput {
		return cli.PrintJSON(os.Stdout, storageDSNResult{Backend: "postgres", DSN: dsn})
	}
	switch *format {
	case "dsn":
		fmt.Fprintln(os.Stdout, dsn)
	case "yaml":
		fmt.Fprintln(os.Stdout, "storage:")
		fmt.Fprintln(os.Stdout, "  backend: postgres")
		fmt.Fprintln(os.Stdout, "  postgres:")
		fmt.Fprintf(os.Stdout, "    dsn: '%s'\n", yamlSingleQuotedValue(dsn))
	}
	return nil
}

func readStorageDSNPassword() (string, error) {
	fmt.Fprint(os.Stderr, "Postgres password: ")
	password, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", fmt.Errorf("read Postgres password: %w", err)
	}
	return string(password), nil
}

func testPostgresDSN(ctx context.Context, dsn string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open postgres connection pool: %w", err)
	}
	defer pool.Close()
	var one int
	if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("test postgres connectivity: %w", err)
	}
	if one != 1 {
		return fmt.Errorf("test postgres connectivity: SELECT 1 returned %d", one)
	}
	return nil
}

func yamlSingleQuotedValue(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func runStorageMigrate(args []string, jsonOutput bool) error {
	fs := newCommandFlagSet("storage migrate", jsonOutput)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	fromSQLite := fs.String("from-sqlite", "", "source SQLite database path")
	tenantID := fs.String("tenant-id", "", "tenant namespace for migrated rows; defaults to config tenant_id")
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
	effectiveTenant := cfg.TenantID
	if *tenantID != "" {
		if strings.TrimSpace(*tenantID) == "" {
			return fmt.Errorf("storage migrate --tenant-id must be a non-empty tenant_id")
		}
		effectiveTenant = *tenantID
	}
	ctx := storage.WithTenant(context.Background(), effectiveTenant)
	if err := storage.MigrateSQLiteToPostgres(
		ctx,
		*fromSQLite,
		cfg.Storage.Postgres.DSN,
		effectiveTenant,
		storage.WithMigrationLogger(func(format string, args ...any) {
			_, _ = fmt.Fprintf(os.Stderr, "migrate sqlite->postgres: "+format+"\n", args...)
		}),
	); err != nil {
		return err
	}
	if jsonOutput {
		return cli.PrintJSON(os.Stdout, storageMigrationResult{Status: "migrated", Source: "sqlite", Target: "postgres", TenantID: effectiveTenant})
	}
	fmt.Fprintln(os.Stdout, "SQLite data migrated to PostgreSQL storage")
	return nil
}

func runMaintenanceAdd(args []string, remote *controlapi.Client, jsonOutput bool) error {
	fs := newCommandFlagSet("maintenance add", jsonOutput)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	monitorID := fs.String("monitor", "", "monitor ID")
	startRaw := fs.String("start", "", "maintenance start time in RFC3339 format")
	endRaw := fs.String("end", "", "maintenance end time in RFC3339 format")
	reason := fs.String("reason", "", "maintenance reason")
	actor := fs.String("by", "", "operator identity for audit records")
	if err := fs.Parse(args); err != nil {
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
	if remote != nil {
		if defaults.FlagWasSet(fs, "config") {
			return fmt.Errorf("maintenance add --config cannot be used with --remote")
		}
		response, err := remote.AddMaintenance(context.Background(), controlapi.AddMaintenanceRequest{MonitorID: *monitorID, StartsAt: start, EndsAt: end, Reason: *reason, CreatedBy: by})
		if err != nil {
			return err
		}
		if jsonOutput {
			return cli.PrintJSON(os.Stdout, response)
		}
		fmt.Fprintf(os.Stdout, "maintenance window %d scheduled for monitor %s\n", response.ID, response.MonitorID)
		return nil
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
	ctx := storage.WithTenant(context.Background(), cfg.TenantID)
	store, err := storage.OpenBackend(ctx, cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()
	id, err := store.AddMaintenanceWindow(ctx, storage.MaintenanceWindow{
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
	if jsonOutput {
		return cli.PrintJSON(os.Stdout, controlapi.AddMaintenanceResponse{ID: id, MonitorID: *monitorID})
	}
	fmt.Fprintf(os.Stdout, "maintenance window %d scheduled for monitor %s\n", id, *monitorID)
	return nil
}

func runMaintenanceCancel(args []string, remote *controlapi.Client, jsonOutput bool) error {
	fs := newCommandFlagSet("maintenance cancel", jsonOutput)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	idRaw := fs.String("id", "", "maintenance window ID")
	reason := fs.String("reason", "", "cancellation reason")
	actor := fs.String("by", "", "operator identity for audit records")
	if err := fs.Parse(args); err != nil {
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
	if remote != nil {
		if defaults.FlagWasSet(fs, "config") {
			return fmt.Errorf("maintenance cancel --config cannot be used with --remote")
		}
		response, err := remote.CancelMaintenance(context.Background(), id, controlapi.CancelMaintenanceRequest{Reason: *reason, CancelledBy: by})
		if err != nil {
			return err
		}
		if jsonOutput {
			return cli.PrintJSON(os.Stdout, response)
		}
		fmt.Fprintf(os.Stdout, "maintenance window %d cancelled\n", response.ID)
		return nil
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
	ctx := storage.WithTenant(context.Background(), cfg.TenantID)
	store, err := storage.OpenBackend(ctx, cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.CancelMaintenanceWindow(ctx, id, time.Now().UTC(), by, *reason); err != nil {
		return err
	}
	if jsonOutput {
		return cli.PrintJSON(os.Stdout, controlapi.CancelMaintenanceResponse{ID: id})
	}
	fmt.Fprintf(os.Stdout, "maintenance window %d cancelled\n", id)
	return nil
}

func runMaintenanceList(args []string, remote *controlapi.Client, jsonOutput bool) error {
	fs := newCommandFlagSet("maintenance list", jsonOutput)
	configPath := fs.String("config", defaults.StandaloneConfigPath, "path to YAML configuration")
	monitorID := fs.String("monitor", "", "monitor ID")
	includeAll := fs.Bool("all", false, "include cancelled and ended maintenance windows")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if remote != nil {
		if defaults.FlagWasSet(fs, "config") {
			return fmt.Errorf("maintenance list --config cannot be used with --remote")
		}
		response, err := remote.Maintenance(context.Background(), *monitorID, *includeAll)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cli.PrintJSON(os.Stdout, response)
		}
		rows := make([]storage.MaintenanceWindow, 0, len(response.Windows))
		for _, window := range response.Windows {
			rows = append(rows, window.Storage())
		}
		return cli.PrintMaintenanceWindows(os.Stdout, rows, response.GeneratedAt)
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
	ctx := storage.WithTenant(context.Background(), cfg.TenantID)
	store, err := storage.OpenBackend(ctx, cfg.Storage)
	if err != nil {
		return err
	}
	defer store.Close()
	now := time.Now().UTC()
	filter := storage.MaintenanceWindowFilter{MonitorID: *monitorID, IncludeAll: *includeAll}
	if !*includeAll {
		filter.Now = now
	}
	windows, err := store.ListMaintenanceWindows(ctx, filter)
	if err != nil {
		return err
	}
	if jsonOutput {
		return cli.PrintJSON(os.Stdout, controlapi.MaintenanceResponse{GeneratedAt: now, Windows: controlapi.MaintenanceFromStorage(windows)})
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

func parseCLISince(raw string, flagName string, now time.Time) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return parsed.UTC(), nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be RFC3339 timestamp or positive duration: %w", flagName, err)
	}
	if duration <= 0 {
		return time.Time{}, fmt.Errorf("%s duration must be positive", flagName)
	}
	return now.UTC().Add(-duration), nil
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
	return fmt.Errorf("usage: upag storage <migrate|dsn> [flags]")
}

func printHelp(w io.Writer) error {
	_, err := fmt.Fprint(w, `upag - lightweight HTTP(S) uptime monitor

Usage:
  upag <command> [flags]
  upag help
  upag --help

Daemon commands:
  run          Run the daemon in the foreground
  start        Start the daemon in the background
  stop         Stop the background daemon
  status       Report whether the background daemon is running
  restart      Restart the background daemon
  config       Manage the running daemon's configuration

Monitoring commands:
  check        Run one diagnostic check without changing stored state
  monitors     List current monitor states
  uptime       Show time since each monitor's latest failure and DOWN incident
  incidents    List recorded incidents
  intervals    List monitor status intervals
  failures     List failed probes and observer failures
  maintenance  Add, cancel, or list maintenance windows

Storage commands:
  storage      Configure or migrate storage

Global options:
  --remote URL             Run a supported command against a remote daemon
  --token TOKEN            Bearer token for the remote daemon
  --remote-timeout DURATION  Remote request timeout (default 1m)
  --local                  Ignore UPAG_REMOTE and run on this host
  --json                   Emit machine-readable JSON for finite commands
  -h, --help               Show this help page
  --version                Print the upag version
`)
	return err
}

func usage() error {
	return fmt.Errorf("usage: upag [global options] <run|start|stop|status|restart|config|check|monitors|uptime|incidents|intervals|failures|maintenance|storage> [flags]; run 'upag --help' for details")
}
