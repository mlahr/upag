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
	if cfg.Alerts.NotificationRetries.MaxAttempts != 3 {
		t.Fatalf("retry max attempts = %d, want 3", cfg.Alerts.NotificationRetries.MaxAttempts)
	}
	if len(cfg.Alerts.NotificationRetries.Backoff) != 3 {
		t.Fatalf("retry backoff count = %d, want 3", len(cfg.Alerts.NotificationRetries.Backoff))
	}
	if cfg.Alerts.NotificationRetries.Backoff[0].Duration != time.Minute {
		t.Fatalf("first retry backoff = %s, want 1m", cfg.Alerts.NotificationRetries.Backoff[0].Duration)
	}
	if cfg.HTTP.Port != 0 {
		t.Fatalf("HTTP port = %d, want 0", cfg.HTTP.Port)
	}
}

func TestParseAcceptsHTTPPort(t *testing.T) {
	cfg, err := Parse([]byte(`
http:
  port: 8080
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
	if cfg.HTTP.Port != 8080 {
		t.Fatalf("HTTP port = %d, want 8080", cfg.HTTP.Port)
	}
}

func TestParseRejectsInvalidHTTPPort(t *testing.T) {
	_, err := Parse([]byte(`
http:
  port: 65536
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
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "http.port must be a TCP port number from 0 through 65535") {
		t.Fatalf("validation error %q does not contain http.port", err)
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

func TestParseAcceptsResponseBodyAssertions(t *testing.T) {
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
    response_body:
      must_contain: "Welcome"
      must_not_contain: "Maintenance mode"
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Monitors[0].ResponseBody.MustContain != "Welcome" {
		t.Fatalf("response_body.must_contain = %q, want Welcome", cfg.Monitors[0].ResponseBody.MustContain)
	}
	if cfg.Monitors[0].ResponseBody.MustNotContain != "Maintenance mode" {
		t.Fatalf("response_body.must_not_contain = %q, want Maintenance mode", cfg.Monitors[0].ResponseBody.MustNotContain)
	}
}

func TestParseAcceptsMaxResponseTime(t *testing.T) {
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
    max_response_time: 500ms
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Monitors[0].MaxResponseTime.Duration != 500*time.Millisecond {
		t.Fatalf("max_response_time = %s, want 500ms", cfg.Monitors[0].MaxResponseTime.Duration)
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

func TestParseRejectsNegativeMaxResponseTime(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
    max_response_time: -1s
`))
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "monitors[0].max_response_time must be positive") {
		t.Fatalf("validation error %q does not contain max_response_time", err)
	}
}

func TestParseRejectsInvalidNotificationRetries(t *testing.T) {
	_, err := Parse([]byte(`
alerts:
  notification_retries:
    max_attempts: -1
    backoff: [0s]
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
	if err == nil {
		t.Fatal("expected validation error")
	}
	message := err.Error()
	for _, want := range []string{"alerts.notification_retries.max_attempts must be positive", "alerts.notification_retries.backoff[0] must be positive"} {
		if !strings.Contains(message, want) {
			t.Fatalf("validation error %q does not contain %q", message, want)
		}
	}
}
