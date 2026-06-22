package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	if err := run([]string{"monitors", "--db", dbPath}); err != nil {
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

func TestRunMonitorsUsesPackagedDBDefault(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "packaged.sqlite")
	withPackageDefaultsPath(t, writeDefaultsFile(t, "UPAG_DB="+dbPath+"\n"))

	if err := run([]string{"monitors"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("stat packaged database: %v", err)
	}
}

func TestRunIncidentsUsesPackagedDBDefault(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "packaged.sqlite")
	withPackageDefaultsPath(t, writeDefaultsFile(t, "UPAG_DB="+dbPath+"\n"))

	if err := run([]string{"incidents"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("stat packaged database: %v", err)
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
	for _, want := range []string{"start", "stop", "status", "restart", "config", "monitors", "incidents"} {
		if !strings.Contains(got, want) {
			t.Fatalf("usage = %q, missing %q", got, want)
		}
	}
}
