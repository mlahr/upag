package cli

import (
	"bytes"
	"testing"
	"time"

	"upag/internal/storage"
)

func TestPrintFailures(t *testing.T) {
	now := time.Date(2026, 6, 28, 10, 0, 0, 0, time.UTC)
	probes := []storage.ProbeResult{
		{MonitorID: "pdfdancer-api", CheckedAt: now, ObservedStatusCode: 500, Error: "timeout"},
		{MonitorID: "pdfdancer-www", CheckedAt: now.Add(-time.Minute), ObservedStatusCode: 0, Error: "timeout", ObserverSuppressed: true},
	}
	observer := storage.ObserverState{
		Status:              "OBSERVER_DOWN",
		ConsecutiveFailures: 2,
	}
	sentinels := []storage.ObserverSentinelResult{
		{SentinelID: "gstatic", OK: false, Error: "connection refused", CheckedAt: now},
	}

	var buf bytes.Buffer
	if err := PrintFailures(&buf, probes, observer, true, sentinels); err != nil {
		t.Fatal(err)
	}
	output := buf.String()
	t.Logf("Output:\n%s", output)

	if !contains(output, "pdfdancer-api") {
		t.Fatal("missing pdfdancer-api")
	}
	if !contains(output, "yes") {
		t.Fatal("missing suppressed=yes")
	}
	if !contains(output, "gstatic") {
		t.Fatal("missing gstatic sentinel")
	}
	if !contains(output, "OBSERVER: OBSERVER_DOWN (2 failures)") {
		t.Fatal("missing observer status line")
	}
}

func TestPrintFailuresNoObserverOutputWhenUp(t *testing.T) {
	now := time.Date(2026, 6, 28, 10, 0, 0, 0, time.UTC)
	probes := []storage.ProbeResult{
		{MonitorID: "test", CheckedAt: now, ObservedStatusCode: 500, Error: "err"},
	}
	observer := storage.ObserverState{
		Status:              "OBSERVER_UP",
		ConsecutiveFailures: 0,
	}

	var buf bytes.Buffer
	if err := PrintFailures(&buf, probes, observer, true, nil); err != nil {
		t.Fatal(err)
	}
	output := buf.String()
	t.Logf("Output:\n%s", output)

	if contains(output, "OBSERVER") {
		t.Fatal("should not show observer section when healthy")
	}
}

func TestPrintFailuresEmpty(t *testing.T) {
	var buf bytes.Buffer
	if err := PrintFailures(&buf, nil, storage.ObserverState{}, false, nil); err != nil {
		t.Fatal(err)
	}
}

func TestPrintStatusIntervals(t *testing.T) {
	now := time.Date(2026, 6, 28, 10, 0, 0, 0, time.UTC)
	intervals := []storage.StatusInterval{
		{MonitorID: "home", Status: "DOWN", StartedAt: now.Add(-time.Hour), EndedAt: now, Downtime: true},
		{MonitorID: "api", Status: "UP", StartedAt: now, Downtime: false},
	}

	var buf bytes.Buffer
	if err := PrintStatusIntervals(&buf, intervals); err != nil {
		t.Fatal(err)
	}
	output := buf.String()
	t.Logf("Output:\n%s", output)

	for _, want := range []string{"START", "END", "DOWNTIME", "STATUS", "MONITOR", "home", "DOWN", "yes", "api", "UP", "no"} {
		if !contains(output, want) {
			t.Fatalf("missing %q in output %q", want, output)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && containsStr(s, substr)
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
