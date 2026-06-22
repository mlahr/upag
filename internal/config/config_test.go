package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseAppliesDefaults(t *testing.T) {
	cfg, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SMTP.Port != 587 {
		t.Fatalf("SMTP port = %d, want 587", cfg.SMTP.Port)
	}
	if cfg.SMTP.TLS != "starttls" {
		t.Fatalf("SMTP TLS = %q, want starttls", cfg.SMTP.TLS)
	}
	if cfg.Defaults.Interval.Duration != time.Minute {
		t.Fatalf("default interval = %s, want 1m", cfg.Defaults.Interval.Duration)
	}
	if cfg.Monitors[0].Timeout.Duration != 10*time.Second {
		t.Fatalf("monitor timeout = %s, want 10s", cfg.Monitors[0].Timeout.Duration)
	}
}

func TestParseAcceptsMailtrapWithoutSMTP(t *testing.T) {
	cfg, err := Parse([]byte(`
mailtrap:
  token: token-123
  from: alerts@example.com
  to: [ops@example.com]
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mailtrap.Endpoint != "https://send.api.mailtrap.io/api/send" {
		t.Fatalf("mailtrap endpoint = %q, want default endpoint", cfg.Mailtrap.Endpoint)
	}
}

func TestParseAcceptsSMTPAndMailtrap(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
mailtrap:
  token: token-123
  from: alerts@example.com
  to: [ops@example.com]
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err != nil {
		t.Fatal(err)
	}
}

func TestParseRejectsMissingAlertProvider(t *testing.T) {
	_, err := Parse([]byte(`
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "at least one alert provider must be configured") {
		t.Fatalf("validation error %q does not contain missing alert provider", err)
	}
}

func TestParseRejectsMissingMailtrapFields(t *testing.T) {
	_, err := Parse([]byte(`
mailtrap:
  endpoint: https://send.api.mailtrap.io/api/send
  from_name: upag
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err == nil {
		t.Fatal("expected validation error")
	}
	message := err.Error()
	for _, want := range []string{"mailtrap.token is required", "mailtrap.from is required", "mailtrap.to must contain at least one recipient"} {
		if !strings.Contains(message, want) {
			t.Fatalf("validation error %q does not contain %q", message, want)
		}
	}
}

func TestParseRejectsMissingSMTPFields(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  username: alerts@example.com
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err == nil {
		t.Fatal("expected validation error")
	}
	message := err.Error()
	for _, want := range []string{"smtp.host is required", "smtp.from is required", "smtp.to must contain at least one recipient"} {
		if !strings.Contains(message, want) {
			t.Fatalf("validation error %q does not contain %q", message, want)
		}
	}
}

func TestParseRejectsInvalidMonitor(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
monitors:
  - id: bad
    name: Bad
    url: ftp://example.com/
    expected_status_code: 99
`))
	if err == nil {
		t.Fatal("expected validation error")
	}
	message := err.Error()
	for _, want := range []string{"scheme must be http or https", "expected_status_code"} {
		if !strings.Contains(message, want) {
			t.Fatalf("validation error %q does not contain %q", message, want)
		}
	}
}
