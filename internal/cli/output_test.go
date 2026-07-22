package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"upag/internal/storage"
)

func TestColorTableStylesHeaderAndSemanticValues(t *testing.T) {
	plain := "STATUS  NAME\nUP      homepage\nDOWN    api\n"
	colored := colorTable(plain)
	for _, want := range []string{
		ansiBoldCyan + "STATUS  NAME" + ansiReset,
		ansiGreen + "UP" + ansiReset,
		ansiRed + "DOWN" + ansiReset,
	} {
		if !strings.Contains(colored, want) {
			t.Fatalf("colored table %q does not contain %q", colored, want)
		}
	}
	if stripped := stripTestANSI(colored); stripped != plain {
		t.Fatalf("color changed table content or alignment: got %q, want %q", stripped, plain)
	}
}

func stripTestANSI(value string) string {
	for _, sequence := range []string{ansiReset, ansiBoldCyan, ansiGreen, ansiYellow, ansiRed, ansiMagenta} {
		value = strings.ReplaceAll(value, sequence, "")
	}
	return value
}

func TestPrintDiagnosticText(t *testing.T) {
	result := DiagnosticResult{
		MonitorID:          "home",
		Name:               "Homepage",
		ConfiguredURL:      "https://example.com/start",
		FinalURL:           "https://example.com/final",
		OK:                 false,
		ExpectedStatusCode: 200,
		ObservedStatusCode: 503,
		RedirectsFollowed:  1,
		LatencyMS:          12,
		ResponseTimeMS:     18,
		CheckedAt:          time.Date(2026, 7, 16, 1, 2, 3, 456, time.UTC),
		Error:              "expected HTTP status 200, observed HTTP status 503",
	}

	var buf bytes.Buffer
	if err := PrintDiagnosticText(&buf, result); err != nil {
		t.Fatal(err)
	}
	output := buf.String()
	for _, want := range []string{
		"MONITOR ID", "home", "NAME", "Homepage", "CONFIGURED URL", result.ConfiguredURL,
		"FINAL URL", result.FinalURL, "OK", "false", "EXPECTED STATUS", "200",
		"OBSERVED STATUS", "503", "REDIRECTS FOLLOWED", "1", "LATENCY MS", "12",
		"RESPONSE TIME MS", "18", "2026-07-16T01:02:03.000000456Z", result.Error,
	} {
		if !contains(output, want) {
			t.Fatalf("text output %q does not contain %q", output, want)
		}
	}
}

func TestPrintDiagnosticJSON(t *testing.T) {
	want := DiagnosticResult{
		MonitorID:          "home",
		Name:               "Homepage",
		ConfiguredURL:      "https://example.com/start?a=1&b=2",
		FinalURL:           "https://example.com/final",
		OK:                 true,
		ExpectedStatusCode: 200,
		ObservedStatusCode: 200,
		RedirectsFollowed:  1,
		LatencyMS:          4,
		ResponseTimeMS:     9,
		CheckedAt:          time.Date(2026, 7, 16, 1, 2, 3, 0, time.UTC),
	}

	var buf bytes.Buffer
	if err := PrintDiagnosticJSON(&buf, want); err != nil {
		t.Fatal(err)
	}
	var got DiagnosticResult
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode JSON output %q: %v", buf.String(), err)
	}
	if got != want {
		t.Fatalf("decoded result = %+v, want %+v", got, want)
	}
	if contains(buf.String(), `\u0026`) {
		t.Fatalf("JSON output unnecessarily HTML-escaped URL: %q", buf.String())
	}
}

func TestPrintDiagnosticTextOmitsEmptyError(t *testing.T) {
	var buf bytes.Buffer
	if err := PrintDiagnosticText(&buf, DiagnosticResult{
		MonitorID: "home",
		OK:        true,
		CheckedAt: time.Date(2026, 7, 16, 1, 2, 3, 0, time.UTC),
	}); err != nil {
		t.Fatal(err)
	}
	if contains(buf.String(), "ERROR") {
		t.Fatalf("successful diagnostic output contains ERROR row: %q", buf.String())
	}
}

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

	for _, want := range []string{"START", "END", "DURATION", "DOWNTIME", "STATUS", "MONITOR", "1h0m0s", "home", "DOWN", "yes", "api", "UP", "no"} {
		if !contains(output, want) {
			t.Fatalf("missing %q in output %q", want, output)
		}
	}
}

func TestPrintStatusIntervalsAtUsesProvidedTimeForOpenInterval(t *testing.T) {
	generatedAt := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	intervals := []storage.StatusInterval{{MonitorID: "home", Status: "DOWN", StartedAt: generatedAt.Add(-90 * time.Minute), Downtime: true}}
	var buf bytes.Buffer
	if err := PrintStatusIntervalsAt(&buf, intervals, generatedAt); err != nil {
		t.Fatal(err)
	}
	if output := buf.String(); !contains(output, "1h30m0s") {
		t.Fatalf("output %q does not use provided generation time", output)
	}
}

func TestPrintUptimeAtShowsIndependentFailureAndDownIncidentAges(t *testing.T) {
	generatedAt := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	monitors := []storage.MonitorUptime{
		{
			MonitorID:         "home",
			Name:              "Homepage",
			Status:            "UP",
			FailureFreeSince:  generatedAt.Add(-90 * time.Minute),
			DowntimeFreeSince: generatedAt.Add(-25 * time.Hour),
		},
		{MonitorID: "api", Name: "API", Status: "UNKNOWN"},
		{MonitorID: "future", Name: "Future", Status: "UP", FailureFreeSince: generatedAt.Add(time.Minute)},
	}

	var buf bytes.Buffer
	if err := PrintUptimeAt(&buf, monitors, generatedAt); err != nil {
		t.Fatal(err)
	}
	output := buf.String()
	for _, want := range []string{
		"UPTIME SINCE LAST FAILURE", "UPTIME SINCE LAST DOWNTIME",
		"home", "Homepage", "1h30m", "1d1h",
		"api", "UNKNOWN", "future",
	} {
		if !contains(output, want) {
			t.Fatalf("uptime output %q does not contain %q", output, want)
		}
	}
	if got := formatElapsedSince(generatedAt.Add(time.Minute), generatedAt); got != "-" {
		t.Fatalf("future event age = %q, want dash", got)
	}
}

func TestFormatElapsedSinceUsesCalendarUnitsAndMinutePrecision(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 3, 30, 0, time.UTC)
	tests := []struct {
		name  string
		event time.Time
		want  string
	}{
		{name: "years months days", event: time.Date(2021, 4, 19, 4, 0, 0, 0, time.UTC), want: "5y3M3d8h3m"},
		{name: "month end clamps", event: time.Date(2026, 1, 31, 12, 3, 30, 0, time.UTC), want: "5M22d"},
		{name: "less than minute", event: now.Add(-30 * time.Second), want: "<1m"},
		{name: "zero", event: now, want: "0m"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := formatElapsedSince(test.event, now); got != test.want {
				t.Fatalf("formatElapsedSince(%s, %s) = %q, want %q", test.event, now, got, test.want)
			}
		})
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
