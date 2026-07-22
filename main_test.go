package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"upag/internal/cli"
	"upag/internal/controlapi"
	"upag/internal/defaults"
	"upag/internal/state"
	"upag/internal/storage"
)

func TestMain(m *testing.M) {
	// CLI tests must not inherit a developer or CI runner's remote target.
	// Individual environment-precedence tests set the variables explicitly.
	for _, key := range []string{"UPAG_REMOTE", "UPAG_TOKEN", "UPAG_REMOTE_TIMEOUT"} {
		_ = os.Unsetenv(key)
	}
	os.Exit(m.Run())
}

func TestRunCheckPrintsRedirectDiagnosticWithoutOpeningStorage(t *testing.T) {
	const responseBody = "ready secret diagnostic body"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/final", http.StatusFound)
			return
		}
		_, _ = w.Write([]byte(responseBody))
	}))
	defer server.Close()

	storagePath := filepath.Join(t.TempDir(), "missing", "diagnostic.sqlite")
	configPath := writeDiagnosticConfig(t, `
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
storage:
  backend: sqlite
  sqlite:
    path: `+storagePath+`
monitors:
  - id: home
    name: Homepage
    url: `+server.URL+`
    expected_status_code: 200
    follow_redirects: true
    max_redirects: 1
    redirect_target:
      exact: `+server.URL+`/final
    response_body:
      must_contain: [ready]
`)
	withPackageDefaultsPath(t, writeDefaultsFile(t, "UPAG_CONFIG="+configPath+"\n"))

	var runErr error
	stdout := captureStdout(t, func() {
		runErr = run([]string{"--json", "check", "--monitor", "home"})
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	var result cli.DiagnosticResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("decode check output %q: %v", stdout, err)
	}
	if !result.OK || result.MonitorID != "home" || result.ObservedStatusCode != http.StatusOK {
		t.Fatalf("diagnostic result = %+v, want successful home check", result)
	}
	if result.FinalURL != server.URL+"/final" || result.RedirectsFollowed != 1 {
		t.Fatalf("diagnostic redirect metadata = URL %q, hops %d", result.FinalURL, result.RedirectsFollowed)
	}
	if strings.Contains(stdout, responseBody) {
		t.Fatalf("diagnostic output exposed response body: %q", stdout)
	}
	stdout = captureStdout(t, func() {
		runErr = run([]string{"check", "--monitor", "home"})
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	for _, want := range []string{"MONITOR ID", "home", "FINAL URL", server.URL + "/final", "OK", "true"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("default text output %q does not contain %q", stdout, want)
		}
	}
	if strings.Contains(stdout, responseBody) {
		t.Fatalf("text diagnostic output exposed response body: %q", stdout)
	}
	if _, err := os.Stat(storagePath); !os.IsNotExist(err) {
		t.Fatalf("diagnostic check created or accessed storage path %q: %v", storagePath, err)
	}
}

func TestRunCheckExecutesCommandOnceWithoutConfiguredRetries(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{"ready":true}`))
	}))
	defer server.Close()

	configPath := writeDiagnosticConfig(t, `
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
defaults:
  probe_retries: 5
  probe_retry_backoff: 1ms
monitors:
  - id: api
    name: API
    url: `+server.URL+`
    expected_status_code: 200
    response_body:
      command: ["sh", "-c", "exit 7"]
`)

	var runErr error
	stdout := captureStdout(t, func() {
		runErr = run([]string{"--json", "check", "--config", configPath, "--monitor", "api"})
	})
	if runErr == nil || runErr.Error() != "diagnostic check failed" {
		t.Fatalf("run error = %v, want diagnostic check failed", runErr)
	}
	if requests.Load() != 1 {
		t.Fatalf("HTTP requests = %d, want exactly one diagnostic attempt", requests.Load())
	}
	var result cli.DiagnosticResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("decode failed check output %q: %v", stdout, err)
	}
	if result.OK || !strings.Contains(result.Error, `response body command "sh" failed with exit status 7`) {
		t.Fatalf("diagnostic result = %+v, want command assertion failure", result)
	}
}

func TestRunCheckValidatesArgumentsAndMonitor(t *testing.T) {
	configPath := writeDiagnosticConfig(t, `
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
monitors:
  - id: home
    name: Homepage
    url: https://example.com/
    expected_status_code: 200
`)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing monitor", args: []string{"check", "--config", configPath}, want: "check requires --monitor"},
		{name: "unknown monitor", args: []string{"check", "--config", configPath, "--monitor", "missing"}, want: `monitor "missing" is not configured`},
		{name: "removed format flag", args: []string{"check", "--config", configPath, "--monitor", "home", "--format", "json"}, want: "flag provided but not defined: -format"},
		{name: "positional argument", args: []string{"check", "home"}, want: "check does not accept positional arguments"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := run(tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run(%v) error = %v, want text %q", tc.args, err, tc.want)
			}
		})
	}
}

func TestRunRecognizesDaemonStatusCommand(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	err := run([]string{"status", "--pid-file", filepath.Join(t.TempDir(), "upag.pid")})
	if err == nil {
		t.Fatal("status returned nil error, want not-running error")
	}
	if !strings.Contains(err.Error(), "daemon is not running") {
		t.Fatalf("status error = %q, want daemon not running", err.Error())
	}
}

func TestRunLocalStatusJSON(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))
	pidFile := filepath.Join(t.TempDir(), "upag.pid")
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	stdout := captureStdout(t, func() {
		if err := run([]string{"--json", "--local", "status", "--pid-file", pidFile}); err != nil {
			t.Fatal(err)
		}
	})
	var result localStatusResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("decode local status JSON %q: %v", stdout, err)
	}
	if result.Status != "running" || result.PID != os.Getpid() || result.PIDFile != pidFile {
		t.Fatalf("result = %+v", result)
	}
}

func TestParseGlobalOptionsUsesFlagsEnvironmentAndLocalOverride(t *testing.T) {
	t.Setenv("UPAG_REMOTE", "https://env.example")
	t.Setenv("UPAG_TOKEN", "env-token")
	options, args, err := parseGlobalOptions([]string{"--token", "flag-token", "monitors"})
	if err != nil {
		t.Fatal(err)
	}
	if options.Remote != "https://env.example" || options.Token != "flag-token" || len(args) != 1 || args[0] != "monitors" {
		t.Fatalf("options = %+v, args = %v", options, args)
	}
	options, args, err = parseGlobalOptions([]string{"--local", "monitors"})
	if err != nil {
		t.Fatal(err)
	}
	if options.Remote != "" || options.Token != "" || len(args) != 1 || args[0] != "monitors" {
		t.Fatalf("local options = %+v, args = %v", options, args)
	}
}

func TestParseGlobalOptionsRecognizesJSON(t *testing.T) {
	options, args, err := parseGlobalOptions([]string{"--json", "monitors"})
	if err != nil {
		t.Fatal(err)
	}
	if !options.JSON || len(args) != 1 || args[0] != "monitors" {
		t.Fatalf("options = %+v, args = %v", options, args)
	}
	if !jsonOutputRequested([]string{"--remote", "https://example.com", "--json", "status"}) {
		t.Fatal("jsonOutputRequested did not recognize --json after a valued global option")
	}
	if jsonOutputRequested([]string{"monitors", "--json"}) {
		t.Fatal("jsonOutputRequested recognized a command-local --json")
	}
}

func TestRunJSONVersionAndUnsupportedCommands(t *testing.T) {
	stdout := captureStdout(t, func() {
		if err := run([]string{"--json", "--version"}); err != nil {
			t.Fatal(err)
		}
	})
	var result versionResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("decode version JSON %q: %v", stdout, err)
	}
	if result.Version != version {
		t.Fatalf("version = %q, want %q", result.Version, version)
	}
	for _, args := range [][]string{{"--json", "run"}, {"--json", "help"}} {
		if err := run(args); err == nil || !strings.Contains(err.Error(), "--json") {
			t.Fatalf("run(%v) error = %v, want JSON compatibility error", args, err)
		}
	}
}

func TestPrintJSONCommandError(t *testing.T) {
	var output strings.Builder
	if err := printRunError(&output, []string{"--json", "monitors"}, io.EOF); err != nil {
		t.Fatal(err)
	}
	var response cli.ErrorResponse
	if err := json.Unmarshal([]byte(output.String()), &response); err != nil {
		t.Fatalf("decode error JSON %q: %v", output.String(), err)
	}
	if response.Error.Code != "command_failed" || response.Error.Message != io.EOF.Error() {
		t.Fatalf("error response = %+v", response)
	}
}

func TestJSONActionResultSchemas(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "start", value: actionResult{Status: "started", PID: 123}, want: `{"status":"started","pid":123}` + "\n"},
		{name: "stop", value: actionResult{Status: "stopped"}, want: `{"status":"stopped"}` + "\n"},
		{name: "local status", value: localStatusResult{Status: "running", PID: 123, PIDFile: "/run/upag.pid"}, want: `{"status":"running","pid":123,"pid_file":"/run/upag.pid"}` + "\n"},
		{name: "storage migration", value: storageMigrationResult{Status: "migrated", Source: "sqlite", Target: "postgres", TenantID: "default"}, want: `{"status":"migrated","source":"sqlite","target":"postgres","tenant_id":"default"}` + "\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var output strings.Builder
			if err := cli.PrintJSON(&output, tc.value); err != nil {
				t.Fatal(err)
			}
			if output.String() != tc.want {
				t.Fatalf("output = %q, want %q", output.String(), tc.want)
			}
		})
	}
}

func TestParseGlobalOptionsRejectsExplicitEmptyRemote(t *testing.T) {
	t.Setenv("UPAG_REMOTE", "")
	t.Setenv("UPAG_TOKEN", "")
	for _, args := range [][]string{{"--remote", "", "monitors"}, {"--remote=", "maintenance", "cancel", "--id", "1"}} {
		_, _, err := parseGlobalOptions(args)
		if err == nil || !strings.Contains(err.Error(), "--remote requires a non-empty URL") {
			t.Fatalf("parseGlobalOptions(%v) error = %v", args, err)
		}
	}
}

func TestRunRemoteStatusPrintsReachabilityMetadata(t *testing.T) {
	started := time.Date(2026, 7, 21, 3, 4, 5, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("authorization = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "version": "v2", "started_at": started})
	}))
	defer server.Close()
	var runErr error
	stdout := captureStdout(t, func() { runErr = run([]string{"--remote", server.URL, "--token", "secret", "status"}) })
	if runErr != nil {
		t.Fatal(runErr)
	}
	for _, want := range []string{server.URL, "v2", started.Format(time.RFC3339Nano), "reachable"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("output %q does not contain %q", stdout, want)
		}
	}
}

func TestRunRemoteStatusJSONUsesAPIShape(t *testing.T) {
	started := time.Date(2026, 7, 21, 3, 4, 5, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(controlapi.StatusResponse{Status: "ok", Version: "v2", StartedAt: started})
	}))
	defer server.Close()
	stdout := captureStdout(t, func() {
		if err := run([]string{"--json", "--remote", server.URL, "--token", "secret", "status"}); err != nil {
			t.Fatal(err)
		}
	})
	var response remoteStatusResult
	if err := json.Unmarshal([]byte(stdout), &response); err != nil {
		t.Fatalf("decode remote status JSON %q: %v", stdout, err)
	}
	if response.Status != "ok" || response.Version != "v2" || !response.StartedAt.Equal(started) || response.RemoteURL != server.URL {
		t.Fatalf("response = %+v", response)
	}
}

func TestRunRemoteIntervalsUsesServerGenerationTime(t *testing.T) {
	generatedAt := time.Date(2020, 1, 2, 3, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/intervals" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(controlapi.IntervalsResponse{
			GeneratedAt: generatedAt,
			Intervals: []controlapi.Interval{{
				MonitorID: "home",
				Status:    "DOWN",
				StartedAt: generatedAt.Add(-90 * time.Minute),
				Downtime:  true,
			}},
		})
	}))
	defer server.Close()
	var runErr error
	stdout := captureStdout(t, func() {
		runErr = run([]string{"--remote", server.URL, "--token", "secret", "intervals"})
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if !strings.Contains(stdout, "1h30m0s") {
		t.Fatalf("output %q does not use server generation time", stdout)
	}
}

func TestRunRemoteUptimeUsesServerGenerationTime(t *testing.T) {
	generatedAt := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	failedAt := generatedAt.Add(-90 * time.Minute)
	downAt := generatedAt.Add(-25 * time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/uptime" {
			t.Fatalf("path = %q, want /v1/uptime", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("authorization = %q", got)
		}
		failedSeconds := int64(5400)
		downSeconds := int64(90000)
		_ = json.NewEncoder(w).Encode(controlapi.UptimeResponse{
			GeneratedAt: generatedAt,
			Monitors: []controlapi.UptimeMonitor{{
				MonitorID:                    "home",
				Name:                         "Homepage",
				Status:                       state.Up,
				LastFailedCheckAt:            &failedAt,
				SecondsSinceLastFailedCheck:  &failedSeconds,
				LastDownIncidentAt:           &downAt,
				SecondsSinceLastDownIncident: &downSeconds,
			}},
		})
	}))
	defer server.Close()

	var runErr error
	stdout := captureStdout(t, func() {
		runErr = run([]string{"--remote", server.URL, "--token", "secret", "uptime"})
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	for _, want := range []string{"home", "Homepage", "UP", "1h30m0s", "25h0m0s", failedAt.Local().Format(time.RFC3339), downAt.Local().Format(time.RFC3339)} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("uptime output %q does not contain %q", stdout, want)
		}
	}
}

func TestRunLocalUptimeJSON(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))
	dbPath := filepath.Join(t.TempDir(), "upag.sqlite")
	configPath := writeSQLiteConfig(t, dbPath)
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	downAt := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	failedAt := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	if _, err := store.SaveProbeAndState(context.Background(), storage.ProbeResult{MonitorID: "home", CheckedAt: downAt, OK: false}, storage.MonitorState{
		MonitorID: "home", Name: "Home", URL: "https://example.com/", Status: state.Down,
		LastCheckedAt: downAt, LastFailureAt: downAt, UpdatedAt: downAt,
	}, &storage.Incident{MonitorID: "home", Name: "Home", Transition: state.Down, ObservedAt: downAt}); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if _, err := store.SaveProbeAndState(context.Background(), storage.ProbeResult{MonitorID: "home", CheckedAt: failedAt, OK: false}, storage.MonitorState{
		MonitorID: "home", Name: "Home", URL: "https://example.com/", Status: state.Failing,
		ConsecutiveFailures: 1, LastCheckedAt: failedAt, LastFailureAt: failedAt, UpdatedAt: failedAt,
	}, nil); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	stdout := captureStdout(t, func() {
		if err := run([]string{"--json", "uptime", "--config", configPath}); err != nil {
			t.Fatal(err)
		}
	})
	var response controlapi.UptimeResponse
	if err := json.Unmarshal([]byte(stdout), &response); err != nil {
		t.Fatalf("decode uptime JSON %q: %v", stdout, err)
	}
	if len(response.Monitors) != 1 {
		t.Fatalf("monitors = %+v", response.Monitors)
	}
	monitor := response.Monitors[0]
	if monitor.MonitorID != "home" || monitor.Status != state.Failing || monitor.LastFailedCheckAt == nil || !monitor.LastFailedCheckAt.Equal(failedAt) || monitor.LastDownIncidentAt == nil || !monitor.LastDownIncidentAt.Equal(downAt) {
		t.Fatalf("monitor = %+v", monitor)
	}
	if monitor.SecondsSinceLastFailedCheck == nil || *monitor.SecondsSinceLastFailedCheck < 1700 || *monitor.SecondsSinceLastFailedCheck > 1900 {
		t.Fatalf("failed-check age = %#v, want approximately 1800", monitor.SecondsSinceLastFailedCheck)
	}
	if monitor.SecondsSinceLastDownIncident == nil || *monitor.SecondsSinceLastDownIncident < 7100 || *monitor.SecondsSinceLastDownIncident > 7300 {
		t.Fatalf("DOWN-incident age = %#v, want approximately 7200", monitor.SecondsSinceLastDownIncident)
	}
}

func TestRunUptimeValidatesArguments(t *testing.T) {
	tests := []struct {
		args []string
		want string
	}{
		{args: []string{"uptime", "home"}, want: "uptime does not accept positional arguments"},
		{args: []string{"--remote", "http://example.com", "--token", "secret", "uptime", "--config", "config.yaml"}, want: "uptime --config cannot be used with --remote"},
	}
	for _, test := range tests {
		if err := run(test.args); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("run(%v) error = %v, want %q", test.args, err, test.want)
		}
	}
}

func TestRunRejectsLocalOnlyCommandInRemoteMode(t *testing.T) {
	err := run([]string{"--remote", "http://example.com", "--token", "secret", "start"})
	if err == nil || !strings.Contains(err.Error(), "cannot run remotely") {
		t.Fatalf("error = %v", err)
	}
}

func TestRunReportsUsageForUnknownCommandInRemoteMode(t *testing.T) {
	err := run([]string{"--remote", "http://example.com", "asdoifhasldfhsf"})
	if err == nil {
		t.Fatal("unknown command returned nil error")
	}
	if err.Error() != usage().Error() {
		t.Fatalf("error = %q, want usage error %q", err, usage())
	}
}

func TestRunRecognizesMonitorsCommand(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	dbPath := filepath.Join(t.TempDir(), "upag.sqlite")
	configPath := writeSQLiteConfig(t, dbPath)
	if err := run([]string{"monitors", "--config", configPath}); err != nil {
		t.Fatal(err)
	}
}

func TestRunJSONQueryCommandsUseControlAPIShapes(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))
	dbPath := filepath.Join(t.TempDir(), "upag.sqlite")
	configPath := writeSQLiteConfig(t, dbPath)
	seedMonitorState(t, dbPath, "home")

	tests := []struct {
		command []string
		key     string
	}{
		{command: []string{"monitors", "--config", configPath}, key: "monitors"},
		{command: []string{"incidents", "--config", configPath}, key: "incidents"},
		{command: []string{"intervals", "--config", configPath}, key: "intervals"},
		{command: []string{"failures", "--config", configPath}, key: "failed_probes"},
		{command: []string{"maintenance", "list", "--config", configPath}, key: "windows"},
	}
	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			args := append([]string{"--json"}, tc.command...)
			stdout := captureStdout(t, func() {
				if err := run(args); err != nil {
					t.Fatal(err)
				}
			})
			var response map[string]json.RawMessage
			if err := json.Unmarshal([]byte(stdout), &response); err != nil {
				t.Fatalf("decode %s JSON %q: %v", tc.key, stdout, err)
			}
			value, ok := response[tc.key]
			if !ok {
				t.Fatalf("JSON output %q has no %q field", stdout, tc.key)
			}
			if string(value) == "null" {
				t.Fatalf("JSON output %q encodes %q as null, want an array", stdout, tc.key)
			}
		})
	}
}

func TestRunRecognizesIntervalsCommand(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	dbPath := filepath.Join(t.TempDir(), "upag.sqlite")
	configPath := writeSQLiteConfig(t, dbPath)
	seedMonitorState(t, dbPath, "home")
	if err := run([]string{"intervals", "--config", configPath, "--monitor", "home", "--limit", "10", "--since", "24h"}); err != nil {
		t.Fatal(err)
	}
}

func TestRunIntervalsPrintsExactMaintenanceWindow(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	dbPath := filepath.Join(t.TempDir(), "upag.sqlite")
	configPath := writeDiagnosticConfig(t, `
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
defaults:
  failure_threshold: 1
storage:
  backend: sqlite
  sqlite:
    path: `+dbPath+`
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`)
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Hour)
	windowStart := base.Add(time.Hour)
	windowEnd := base.Add(2 * time.Hour)
	if _, err := store.SaveProbeAndState(context.Background(), storage.ProbeResult{
		MonitorID: "home",
		CheckedAt: base,
		OK:        false,
	}, storage.MonitorState{
		MonitorID:     "home",
		Name:          "Home",
		URL:           "https://example.com/",
		Status:        state.Down,
		LastCheckedAt: base,
		UpdatedAt:     base,
	}, nil); err != nil {
		t.Fatal(err)
	}
	windowID, err := store.AddMaintenanceWindow(context.Background(), storage.MaintenanceWindow{
		MonitorID: "home",
		StartsAt:  windowStart,
		EndsAt:    windowEnd,
		Reason:    "deploy",
		CreatedBy: "tester",
		CreatedAt: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveProbeAndState(context.Background(), storage.ProbeResult{
		MonitorID:           "home",
		CheckedAt:           windowStart.Add(5 * time.Minute),
		OK:                  false,
		MaintenanceWindowID: windowID,
	}, storage.MonitorState{
		MonitorID:               "home",
		Name:                    "Home",
		URL:                     "https://example.com/",
		Status:                  state.Maintenance,
		StatusBeforeMaintenance: state.Down,
		LastCheckedAt:           windowStart.Add(5 * time.Minute),
		UpdatedAt:               windowStart.Add(5 * time.Minute),
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	var runErr error
	stdout := captureStdout(t, func() {
		runErr = run([]string{"intervals", "--config", configPath, "--monitor", "home", "--limit", "10"})
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	for _, want := range []string{
		"MAINTENANCE",
		windowStart.Local().Format(time.RFC3339),
		windowEnd.Local().Format(time.RFC3339),
		"no",
		"DOWN",
		"yes",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("interval output %q does not contain %q", stdout, want)
		}
	}
}

func TestRunRecognizesSinceOnLimitedCommands(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	dbPath := filepath.Join(t.TempDir(), "upag.sqlite")
	configPath := writeSQLiteConfig(t, dbPath)
	seedMonitorState(t, dbPath, "home")

	for _, args := range [][]string{
		{"incidents", "--config", configPath, "--limit", "10", "--since", "2026-06-23T00:00:00Z"},
		{"failures", "--config", configPath, "--limit", "10", "--since", "24h"},
	} {
		if err := run(args); err != nil {
			t.Fatalf("run(%v): %v", args, err)
		}
	}
}

func TestParseCLISince(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		raw  string
		want time.Time
	}{
		{name: "empty", raw: "", want: time.Time{}},
		{name: "timestamp", raw: "2026-06-23T01:02:03Z", want: time.Date(2026, 6, 23, 1, 2, 3, 0, time.UTC)},
		{name: "duration", raw: "24h", want: now.Add(-24 * time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCLISince(tc.raw, "--since", now)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(tc.want) {
				t.Fatalf("parseCLISince(%q) = %s, want %s", tc.raw, got, tc.want)
			}
		})
	}

	for _, raw := range []string{"nope", "0s", "-1h"} {
		if _, err := parseCLISince(raw, "--since", now); err == nil {
			t.Fatalf("parseCLISince(%q) returned nil error, want validation error", raw)
		}
	}
}

func TestRunRecognizesConfigReloadCommand(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	err := run([]string{"config", "reload", "--pid-file", filepath.Join(t.TempDir(), "upag.pid")})
	if err == nil {
		t.Fatal("config reload returned nil error, want not-running error")
	}
	if !strings.Contains(err.Error(), "daemon is not running") {
		t.Fatalf("config reload error = %q, want daemon not running", err.Error())
	}
}

func TestRunRejectsUnknownConfigCommand(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	err := run([]string{"config", "unknown"})
	if err == nil {
		t.Fatal("unknown config command returned nil error")
	}
	if !strings.Contains(err.Error(), "usage: upag config <reload>") {
		t.Fatalf("config command error = %q, want config usage", err.Error())
	}
}

func TestRunRejectsOldStatusDBFlag(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	err := run([]string{"status", "--db", filepath.Join(t.TempDir(), "upag.sqlite")})
	if err == nil {
		t.Fatal("status --db returned nil error, want flag error")
	}
}

func TestRunMonitorsUsesPackagedConfigDefault(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "packaged.sqlite")
	configPath := writeSQLiteConfig(t, dbPath)
	withPackageDefaultsPath(t, writeDefaultsFile(t, "UPAG_CONFIG="+configPath+"\n"))

	if err := run([]string{"monitors"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("stat packaged database: %v", err)
	}
}

func TestRunIncidentsUsesPackagedConfigDefault(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "packaged.sqlite")
	configPath := writeSQLiteConfig(t, dbPath)
	withPackageDefaultsPath(t, writeDefaultsFile(t, "UPAG_CONFIG="+configPath+"\n"))

	if err := run([]string{"incidents"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("stat packaged database: %v", err)
	}
}

func TestRunRecognizesMaintenanceCommands(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	dbPath := filepath.Join(t.TempDir(), "upag.sqlite")
	configPath := writeSQLiteConfig(t, dbPath)
	seedMonitorState(t, dbPath, "home")
	if err := run([]string{
		"maintenance", "add",
		"--config", configPath,
		"--monitor", "home",
		"--start", "2026-06-23T01:00:00Z",
		"--end", "2026-06-23T02:00:00Z",
		"--reason", "deploy",
		"--by", "tester",
	}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"maintenance", "list", "--config", configPath, "--all"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"maintenance", "cancel", "--config", configPath, "--id", "1", "--reason", "done", "--by", "tester"}); err != nil {
		t.Fatal(err)
	}
}

func TestRunRejectsUnknownMaintenanceCommand(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	err := run([]string{"maintenance", "unknown"})
	if err == nil {
		t.Fatal("unknown maintenance command returned nil error")
	}
	if !strings.Contains(err.Error(), "usage: upag maintenance <add|cancel|list>") {
		t.Fatalf("maintenance command error = %q, want maintenance usage", err.Error())
	}
}

func TestRunRecognizesStorageMigrateCommandValidation(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	err := run([]string{"storage", "migrate", "--config", writeSQLiteConfig(t, filepath.Join(t.TempDir(), "upag.sqlite"))})
	if err == nil || !strings.Contains(err.Error(), "storage migrate requires --from-sqlite") {
		t.Fatalf("storage migrate error = %v, want missing --from-sqlite", err)
	}
}

func TestBuildPostgresDSNEncodesUserAndPassword(t *testing.T) {
	dsn, err := buildPostgresDSN(postgresDSNOptions{
		Host:     "db.example.supabase.co",
		User:     "person@example.com",
		Password: "abc@123:xyz/?#% '",
		Database: "postgres",
		Port:     5432,
		SSLMode:  "require",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "postgres://person%40example.com:abc%40123%3Axyz%2F%3F%23%25%20%27@db.example.supabase.co:5432/postgres?sslmode=require"
	if dsn != want {
		t.Fatalf("dsn = %q, want %q", dsn, want)
	}
}

func TestRunStorageDSNPrintsDSNWithoutTesting(t *testing.T) {
	restorePassword := storageDSNReadPassword
	storageDSNReadPassword = func() (string, error) { return "abc@123:xyz", nil }
	t.Cleanup(func() { storageDSNReadPassword = restorePassword })

	stdout := captureStdout(t, func() {
		if err := run([]string{
			"storage", "dsn",
			"--host", "db.example.supabase.co",
			"--user", "person@example.com",
			"--no-test",
			"--format", "dsn",
		}); err != nil {
			t.Fatal(err)
		}
	})
	want := "postgres://person%40example.com:abc%40123%3Axyz@db.example.supabase.co:5432/postgres?sslmode=require\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

func TestRunStorageDSNPrintsYAMLSnippetWithoutTesting(t *testing.T) {
	restorePassword := storageDSNReadPassword
	storageDSNReadPassword = func() (string, error) { return "abc@123:xyz", nil }
	t.Cleanup(func() { storageDSNReadPassword = restorePassword })

	stdout := captureStdout(t, func() {
		if err := run([]string{
			"storage", "dsn",
			"--host", "db.example.supabase.co",
			"--user", "person@example.com",
			"--no-test",
		}); err != nil {
			t.Fatal(err)
		}
	})
	want := "storage:\n  backend: postgres\n  postgres:\n    dsn: 'postgres://person%40example.com:abc%40123%3Axyz@db.example.supabase.co:5432/postgres?sslmode=require'\n"
	if stdout != want {
		t.Fatalf("stdout = %q, want %q", stdout, want)
	}
}

func TestRunStorageDSNPrintsJSONWithoutTesting(t *testing.T) {
	restorePassword := storageDSNReadPassword
	storageDSNReadPassword = func() (string, error) { return "abc@123:xyz", nil }
	t.Cleanup(func() { storageDSNReadPassword = restorePassword })

	stdout := captureStdout(t, func() {
		if err := run([]string{
			"--json", "storage", "dsn",
			"--host", "db.example.supabase.co",
			"--user", "person@example.com",
			"--no-test",
		}); err != nil {
			t.Fatal(err)
		}
	})
	var result storageDSNResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("decode storage DSN JSON %q: %v", stdout, err)
	}
	if result.Backend != "postgres" || result.DSN != "postgres://person%40example.com:abc%40123%3Axyz@db.example.supabase.co:5432/postgres?sslmode=require" {
		t.Fatalf("result = %+v", result)
	}
}

func TestRunStorageDSNRejectsFormatWithJSON(t *testing.T) {
	err := run([]string{
		"--json", "storage", "dsn",
		"--host", "db.example.supabase.co",
		"--user", "person@example.com",
		"--no-test",
		"--format", "dsn",
	})
	if err == nil || !strings.Contains(err.Error(), "--format cannot be combined with --json") {
		t.Fatalf("error = %v, want format conflict", err)
	}
}

func TestRunStorageDSNValidatesRequiredFlags(t *testing.T) {
	restorePassword := storageDSNReadPassword
	storageDSNReadPassword = func() (string, error) { return "password", nil }
	t.Cleanup(func() { storageDSNReadPassword = restorePassword })

	err := run([]string{"storage", "dsn", "--user", "person@example.com", "--no-test"})
	if err == nil || !strings.Contains(err.Error(), "storage dsn requires --host") {
		t.Fatalf("storage dsn error = %v, want missing host", err)
	}
	err = run([]string{"storage", "dsn", "--host", "db.example.supabase.co", "--no-test"})
	if err == nil || !strings.Contains(err.Error(), "storage dsn requires --user") {
		t.Fatalf("storage dsn error = %v, want missing user", err)
	}
}

func TestRunStorageDSNValidatesFormat(t *testing.T) {
	err := run([]string{
		"storage", "dsn",
		"--host", "db.example.supabase.co",
		"--user", "person@example.com",
		"--no-test",
		"--format", "json",
	})
	if err == nil || !strings.Contains(err.Error(), "storage dsn --format must be one of: yaml, dsn") {
		t.Fatalf("storage dsn error = %v, want invalid format", err)
	}
}

func TestRunRejectsUnknownStorageCommand(t *testing.T) {
	err := run([]string{"storage", "unknown"})
	if err == nil {
		t.Fatal("unknown storage command returned nil error")
	}
	if !strings.Contains(err.Error(), "usage: upag storage <migrate|dsn>") {
		t.Fatalf("storage command error = %q, want storage usage", err.Error())
	}
}

func TestRunStatusUsesPackagedPIDFileDefault(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "packaged.pid")
	if err := os.WriteFile(pidFile, []byte("999999\n"), 0644); err != nil {
		t.Fatal(err)
	}
	withPackageDefaultsPath(t, writeDefaultsFile(t, "UPAG_PIDFILE="+pidFile+"\n"))

	err := run([]string{"status"})
	if err == nil {
		t.Fatal("status returned nil error, want stale pid error")
	}
	if !strings.Contains(err.Error(), pidFile) {
		t.Fatalf("status error = %q, want packaged pid file path", err.Error())
	}
}

func TestRunStatusExplicitPIDFileOverridesPackagedDefault(t *testing.T) {
	packagedPIDFile := filepath.Join(t.TempDir(), "packaged.pid")
	if err := os.WriteFile(packagedPIDFile, []byte("999999\n"), 0644); err != nil {
		t.Fatal(err)
	}
	explicitPIDFile := filepath.Join(t.TempDir(), "explicit.pid")
	withPackageDefaultsPath(t, writeDefaultsFile(t, "UPAG_PIDFILE="+packagedPIDFile+"\n"))

	err := run([]string{"status", "--pid-file", explicitPIDFile})
	if err == nil {
		t.Fatal("status returned nil error, want not-running error")
	}
	if strings.Contains(err.Error(), packagedPIDFile) {
		t.Fatalf("status error = %q, used packaged pid file despite explicit flag", err.Error())
	}
}

func TestUsageMentionsDaemonAndMonitorCommands(t *testing.T) {
	err := usage()
	if err == nil {
		t.Fatal("usage returned nil error")
	}
	got := err.Error()
	for _, want := range []string{"start", "stop", "status", "restart", "config", "check", "monitors", "incidents", "maintenance", "storage"} {
		if !strings.Contains(got, want) {
			t.Fatalf("usage = %q, missing %q", got, want)
		}
	}
}

func TestRunPrintsHelp(t *testing.T) {
	var outputs []string
	for _, arg := range []string{"help", "--help", "-h"} {
		var runErr error
		output := captureStdout(t, func() {
			runErr = run([]string{arg})
		})
		if runErr != nil {
			t.Fatalf("run(%q) error = %v, want nil", arg, runErr)
		}
		for _, want := range []string{
			"upag - lightweight HTTP(S) uptime monitor",
			"Usage:",
			"Daemon commands:",
			"Monitoring commands:",
			"Storage commands:",
			"Global options:",
			"--json",
			"--version",
		} {
			if !strings.Contains(output, want) {
				t.Errorf("run(%q) output is missing %q:\n%s", arg, want, output)
			}
		}
		outputs = append(outputs, output)
	}
	if outputs[0] != outputs[1] || outputs[0] != outputs[2] {
		t.Fatal("help, --help, and -h produced different help pages")
	}
}

func TestRunHelpRejectsArguments(t *testing.T) {
	err := run([]string{"help", "check"})
	if err == nil || err.Error() != "help does not accept arguments" {
		t.Fatalf("run help error = %v, want help argument error", err)
	}
}

func writeSQLiteConfig(t *testing.T, dbPath string) string {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
storage:
  backend: sqlite
  sqlite:
    path: `+dbPath+`
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`), 0644); err != nil {
		t.Fatal(err)
	}
	return configPath
}

func writeDiagnosticConfig(t *testing.T, contents string) string {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "diagnostic.yaml")
	if err := os.WriteFile(configPath, []byte(contents), 0644); err != nil {
		t.Fatal(err)
	}
	return configPath
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	original := os.Stdout
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = write
	defer func() { os.Stdout = original }()

	fn()

	if err := write.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(read)
	if err != nil {
		t.Fatal(err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	return string(output)
}

func seedMonitorState(t *testing.T, dbPath string, monitorID string) {
	t.Helper()
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	_, err = store.SaveProbeAndState(context.Background(), storage.ProbeResult{
		MonitorID: monitorID,
		CheckedAt: now,
		OK:        true,
	}, storage.MonitorState{
		MonitorID:              monitorID,
		Name:                   monitorID,
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 state.Up,
		LastCheckedAt:          now,
		LastSuccessAt:          now,
		LastObservedStatusCode: 200,
		UpdatedAt:              now,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func withPackageDefaultsPath(t *testing.T, path string) {
	t.Helper()
	restore := defaults.SetPackageDefaultsPathForTest(path)
	t.Cleanup(restore)
}

func writeDefaultsFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "upag.default")
	if err := os.WriteFile(path, []byte(contents), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}
