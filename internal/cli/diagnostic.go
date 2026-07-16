package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"
)

type DiagnosticResult struct {
	MonitorID          string    `json:"monitor_id"`
	Name               string    `json:"name"`
	ConfiguredURL      string    `json:"configured_url"`
	FinalURL           string    `json:"final_url"`
	OK                 bool      `json:"ok"`
	ExpectedStatusCode int       `json:"expected_status_code"`
	ObservedStatusCode int       `json:"observed_status_code"`
	RedirectsFollowed  int       `json:"redirects_followed"`
	LatencyMS          int64     `json:"latency_ms"`
	ResponseTimeMS     int64     `json:"response_time_ms"`
	CheckedAt          time.Time `json:"checked_at"`
	Error              string    `json:"error"`
}

func PrintDiagnosticText(w io.Writer, result DiagnosticResult) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	type field struct {
		label string
		value any
	}
	fields := []field{
		{label: "MONITOR ID", value: result.MonitorID},
		{label: "NAME", value: result.Name},
		{label: "CONFIGURED URL", value: result.ConfiguredURL},
		{label: "FINAL URL", value: emptyDash(result.FinalURL)},
		{label: "OK", value: result.OK},
		{label: "EXPECTED STATUS", value: result.ExpectedStatusCode},
		{label: "OBSERVED STATUS", value: result.ObservedStatusCode},
		{label: "REDIRECTS FOLLOWED", value: result.RedirectsFollowed},
		{label: "LATENCY MS", value: result.LatencyMS},
		{label: "RESPONSE TIME MS", value: result.ResponseTimeMS},
		{label: "CHECKED AT", value: result.CheckedAt.UTC().Format(time.RFC3339Nano)},
	}
	if result.Error != "" {
		fields = append(fields, field{label: "ERROR", value: result.Error})
	}
	for _, field := range fields {
		if _, err := fmt.Fprintf(tw, "%s\t%v\n", field.label, field.value); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func PrintDiagnosticJSON(w io.Writer, result DiagnosticResult) error {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
}
