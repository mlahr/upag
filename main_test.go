package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRunRecognizesDaemonStatusCommand(t *testing.T) {
	err := run([]string{"status", "--pid-file", filepath.Join(t.TempDir(), "upag.pid")})
	if err == nil {
		t.Fatal("status returned nil error, want not-running error")
	}
	if !strings.Contains(err.Error(), "daemon is not running") {
		t.Fatalf("status error = %q, want daemon not running", err.Error())
	}
}

func TestRunRecognizesMonitorsCommand(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "upag.sqlite")
	if err := run([]string{"monitors", "--db", dbPath}); err != nil {
		t.Fatal(err)
	}
}

func TestRunRejectsOldStatusDBFlag(t *testing.T) {
	err := run([]string{"status", "--db", filepath.Join(t.TempDir(), "upag.sqlite")})
	if err == nil {
		t.Fatal("status --db returned nil error, want flag error")
	}
}

func TestUsageMentionsDaemonAndMonitorCommands(t *testing.T) {
	err := usage()
	if err == nil {
		t.Fatal("usage returned nil error")
	}
	got := err.Error()
	for _, want := range []string{"start", "stop", "status", "restart", "monitors", "incidents"} {
		if !strings.Contains(got, want) {
			t.Fatalf("usage = %q, missing %q", got, want)
		}
	}
}
