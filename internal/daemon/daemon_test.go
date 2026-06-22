package daemon

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestInspectMissingPIDFileReportsNotRunning(t *testing.T) {
	status, err := Inspect(filepath.Join(t.TempDir(), "missing.pid"))
	if err != nil {
		t.Fatal(err)
	}
	if status.Running {
		t.Fatalf("Running = true, want false")
	}
	if status.StaleFile {
		t.Fatalf("StaleFile = true, want false")
	}
}

func TestInspectCurrentProcessReportsRunning(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "upag.pid")
	if err := os.WriteFile(pidFile, []byte(" "+strconv.Itoa(os.Getpid())+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	status, err := Inspect(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Running {
		t.Fatalf("Running = false, want true")
	}
	if status.PID != os.Getpid() {
		t.Fatalf("PID = %d, want %d", status.PID, os.Getpid())
	}
}

func TestInspectStalePIDFile(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "upag.pid")
	if err := os.WriteFile(pidFile, []byte("999999\n"), 0644); err != nil {
		t.Fatal(err)
	}

	status, err := Inspect(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	if status.Running {
		t.Fatalf("Running = true, want false")
	}
	if !status.StaleFile {
		t.Fatalf("StaleFile = false, want true")
	}
}

func TestReadPIDRejectsInvalidPIDFile(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "upag.pid")
	if err := os.WriteFile(pidFile, []byte("not-a-pid\n"), 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := ReadPID(pidFile); err == nil {
		t.Fatal("ReadPID returned nil error, want invalid PID error")
	}
}
