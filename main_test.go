package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"upag/internal/defaults"
	"upag/internal/state"
	"upag/internal/storage"
)

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

func TestRunRecognizesMonitorsCommand(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	dbPath := filepath.Join(t.TempDir(), "upag.sqlite")
	configPath := writeSQLiteConfig(t, dbPath)
	if err := run([]string{"monitors", "--config", configPath}); err != nil {
		t.Fatal(err)
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
	for _, want := range []string{"start", "stop", "status", "restart", "config", "monitors", "incidents", "maintenance", "storage"} {
		if !strings.Contains(got, want) {
			t.Fatalf("usage = %q, missing %q", got, want)
		}
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
