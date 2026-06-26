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
	if cfg.Defaults.ProbeRetries != 2 {
		t.Fatalf("default probe retries = %d, want 2", cfg.Defaults.ProbeRetries)
	}
	if cfg.Defaults.ProbeRetryBackoff.Duration != 500*time.Millisecond {
		t.Fatalf("default probe retry backoff = %s, want 500ms", cfg.Defaults.ProbeRetryBackoff.Duration)
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
	if cfg.HTTP.Address != "127.0.0.1" {
		t.Fatalf("HTTP address = %q, want 127.0.0.1", cfg.HTTP.Address)
	}
	if cfg.Storage.ProbeResults.Retention.Duration != 30*24*time.Hour || cfg.Storage.ProbeResults.Retention.Forever {
		t.Fatalf("probe_results retention = %+v, want 30d finite", cfg.Storage.ProbeResults.Retention)
	}
	if cfg.Storage.Backend != "sqlite" {
		t.Fatalf("storage backend = %q, want sqlite", cfg.Storage.Backend)
	}
	if cfg.Storage.SQLite.Path != "./upag.sqlite" {
		t.Fatalf("storage sqlite path = %q, want ./upag.sqlite", cfg.Storage.SQLite.Path)
	}
	if cfg.Storage.ProbeMinuteRollups.Retention.Duration != 30*24*time.Hour || cfg.Storage.ProbeMinuteRollups.Retention.Forever {
		t.Fatalf("probe_minute_rollups retention = %+v, want 30d finite", cfg.Storage.ProbeMinuteRollups.Retention)
	}
	if cfg.Storage.ProbeHourlyRollups.Retention.Duration != 365*24*time.Hour || cfg.Storage.ProbeHourlyRollups.Retention.Forever {
		t.Fatalf("probe_hourly_rollups retention = %+v, want 1y finite", cfg.Storage.ProbeHourlyRollups.Retention)
	}
	if !cfg.Storage.ProbeDailyRollups.Retention.Forever {
		t.Fatalf("probe_daily_rollups retention = %+v, want forever", cfg.Storage.ProbeDailyRollups.Retention)
	}
	if !cfg.Observer.Enabled {
		t.Fatal("observer enabled = false, want true")
	}
	if cfg.Observer.Interval.Duration != 30*time.Second {
		t.Fatalf("observer interval = %s, want 30s", cfg.Observer.Interval.Duration)
	}
	if cfg.Observer.Timeout.Duration != 5*time.Second {
		t.Fatalf("observer timeout = %s, want 5s", cfg.Observer.Timeout.Duration)
	}
	if cfg.Observer.FailureThreshold != 3 || cfg.Observer.RecoveryThreshold != 1 || cfg.Observer.RequiredSuccesses != 1 {
		t.Fatalf("observer thresholds = failure:%d recovery:%d required:%d, want 3, 1, 1", cfg.Observer.FailureThreshold, cfg.Observer.RecoveryThreshold, cfg.Observer.RequiredSuccesses)
	}
	if len(cfg.Observer.Sentinels) != 4 {
		t.Fatalf("observer sentinel count = %d, want 4", len(cfg.Observer.Sentinels))
	}
	if cfg.Observer.Sentinels[2].ID != "cloudflare-ip" || cfg.Observer.Sentinels[2].URL != "http://1.1.1.1/cdn-cgi/trace" || cfg.Observer.Sentinels[2].ExpectedStatusCode != 301 {
		t.Fatalf("observer IP sentinel = %+v, want cloudflare-ip over literal IPv4 with HTTP 301", cfg.Observer.Sentinels[2])
	}
	if cfg.TenantID != "default" {
		t.Fatalf("tenant_id = %q, want default", cfg.TenantID)
	}
}

func TestParseAcceptsTenantID(t *testing.T) {
	cfg, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
tenant_id: team-blue
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TenantID != "team-blue" {
		t.Fatalf("tenant_id = %q, want team-blue", cfg.TenantID)
	}
}

func TestParseRejectsWhitespaceOnlyTenantID(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
tenant_id: "   "
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "tenant_id is required") {
		t.Fatalf("parse error = %v, want tenant_id is required", err)
	}
}

func TestParseRejectsMalformedTenantIDCharacters(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
tenant_id: "tenant blue"
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "tenant_id must match") {
		t.Fatalf("parse error = %v, want tenant_id policy mismatch", err)
	}
}

func TestParseRejectsTenantIDWithLeadingOrTrailingWhitespace(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
tenant_id: " team-blue "
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "tenant_id must not have leading or trailing whitespace") {
		t.Fatalf("parse error = %v, want tenant_id whitespace mismatch", err)
	}
}

func TestParseRejectsUnicodeTenantID(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
tenant_id: "ténant"
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "tenant_id must match") {
		t.Fatalf("parse error = %v, want tenant_id policy mismatch", err)
	}
}

func TestParseAcceptsTenantIDWithAllowedSymbols(t *testing.T) {
	cfg, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
tenant_id: team-blue_01
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TenantID != "team-blue_01" {
		t.Fatalf("tenant_id = %q, want team-blue_01", cfg.TenantID)
	}
}

func TestParsePostgresStorage(t *testing.T) {
	cfg, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
storage:
  backend: postgres
  postgres:
    dsn: postgres://user:password@localhost:5432/upag?sslmode=disable
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.Backend != "postgres" {
		t.Fatalf("storage backend = %q, want postgres", cfg.Storage.Backend)
	}
	if cfg.Storage.Postgres.DSN != "postgres://user:password@localhost:5432/upag?sslmode=disable" {
		t.Fatalf("postgres dsn = %q", cfg.Storage.Postgres.DSN)
	}
}

func TestParseRejectsInvalidStorageBackend(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
storage:
  backend: supabase
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err == nil || !strings.Contains(err.Error(), "storage.backend must be one of: sqlite, postgres") {
		t.Fatalf("Parse error = %v, want invalid storage backend", err)
	}
}

func TestParseRejectsMissingPostgresDSN(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
storage:
  backend: postgres
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err == nil || !strings.Contains(err.Error(), "storage.postgres.dsn is required") {
		t.Fatalf("Parse error = %v, want missing postgres dsn", err)
	}
}

func TestParseStorageRetention(t *testing.T) {
	cfg, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
storage:
  probe_results:
    retention: 24h
  probe_minute_rollups:
    retention: 30d
  probe_hourly_rollups:
    retention: 1y
  probe_daily_rollups:
    retention: forever
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.ProbeResults.Retention.Duration != 24*time.Hour {
		t.Fatalf("probe_results retention = %s, want 24h", cfg.Storage.ProbeResults.Retention.Duration)
	}
	if cfg.Storage.ProbeMinuteRollups.Retention.Duration != 30*24*time.Hour {
		t.Fatalf("probe_minute_rollups retention = %s, want 720h", cfg.Storage.ProbeMinuteRollups.Retention.Duration)
	}
	if cfg.Storage.ProbeHourlyRollups.Retention.Duration != 365*24*time.Hour {
		t.Fatalf("probe_hourly_rollups retention = %s, want 8760h", cfg.Storage.ProbeHourlyRollups.Retention.Duration)
	}
	if !cfg.Storage.ProbeDailyRollups.Retention.Forever {
		t.Fatal("probe_daily_rollups retention forever = false, want true")
	}
}

func TestParseStorageRetentionFallsBackToHistoryRetention(t *testing.T) {
	cfg, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
defaults:
  history_retention: 12h
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.ProbeResults.Retention.Duration != 12*time.Hour {
		t.Fatalf("probe_results retention = %s, want history_retention fallback 12h", cfg.Storage.ProbeResults.Retention.Duration)
	}
}

func TestParseAcceptsZeroProbeRetries(t *testing.T) {
	cfg, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
defaults:
  probe_retries: 0
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Defaults.ProbeRetries != 0 {
		t.Fatalf("probe retries = %d, want 0", cfg.Defaults.ProbeRetries)
	}
}

func TestParseRejectsNegativeProbeRetries(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
defaults:
  probe_retries: -1
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "defaults.probe_retries must be non-negative") {
		t.Fatalf("validation error %q does not contain defaults.probe_retries", err)
	}
}

func TestParseAcceptsDisabledObserverWithoutSentinels(t *testing.T) {
	cfg, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
observer:
  enabled: false
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Observer.Enabled {
		t.Fatal("observer enabled = true, want false")
	}
}

func TestParseRejectsObserverQuorumLargerThanSentinels(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
observer:
  required_successes: 2
  sentinels:
    - id: one
      name: One
      url: https://example.com/
      expected_status_code: 200
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "observer.required_successes") {
		t.Fatalf("validation error %q does not contain observer.required_successes", err)
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
	if cfg.HTTP.Address != "127.0.0.1" {
		t.Fatalf("HTTP address = %q, want default 127.0.0.1", cfg.HTTP.Address)
	}
}

func TestParseAcceptsHTTPAddress(t *testing.T) {
	for _, address := range []string{"0.0.0.0", "::1", "localhost"} {
		t.Run(address, func(t *testing.T) {
			cfg, err := Parse([]byte(`
http:
  address: "` + address + `"
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
			if cfg.HTTP.Address != address {
				t.Fatalf("HTTP address = %q, want %q", cfg.HTTP.Address, address)
			}
		})
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

func TestParseRejectsInvalidHTTPAddress(t *testing.T) {
	tests := map[string]string{
		"with-port":           "127.0.0.1:8080",
		"ipv6-with-port":      "[::1]:8080",
		"leading-whitespace":  " 127.0.0.1",
		"trailing-whitespace": "127.0.0.1 ",
	}
	for name, address := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(`
http:
  address: "` + address + `"
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
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), "http.address") {
				t.Fatalf("validation error %q does not contain http.address", err)
			}
		})
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

func TestParseAcceptsAlertsProvidersTelegram(t *testing.T) {
	cfg, err := Parse([]byte(`
alerts:
  providers:
    telegram:
      token: token-123
      chat_ids: ["123456789"]
monitors:
  - id: home
    name: Home
    url: https://example.com/
    expected_status_code: 200
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Alerts.Providers.Telegram.Endpoint != "https://api.telegram.org" {
		t.Fatalf("telegram endpoint = %q, want default endpoint", cfg.Alerts.Providers.Telegram.Endpoint)
	}
}

func TestParseAcceptsAlertsProvidersSMTPAndMailtrap(t *testing.T) {
	cfg, err := Parse([]byte(`
alerts:
  providers:
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
	if cfg.Alerts.Providers.SMTP.Port != 587 {
		t.Fatalf("provider SMTP port = %d, want 587", cfg.Alerts.Providers.SMTP.Port)
	}
	if cfg.Alerts.Providers.SMTP.TLS != "starttls" {
		t.Fatalf("provider SMTP TLS = %q, want starttls", cfg.Alerts.Providers.SMTP.TLS)
	}
	if cfg.Alerts.Providers.Mailtrap.Endpoint != "https://send.api.mailtrap.io/api/send" {
		t.Fatalf("provider mailtrap endpoint = %q, want default endpoint", cfg.Alerts.Providers.Mailtrap.Endpoint)
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
      command: ["jq", "-e", ".ok == true"]
      command_timeout: 2s
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
	if strings.Join(cfg.Monitors[0].ResponseBody.Command, " ") != "jq -e .ok == true" {
		t.Fatalf("response_body.command = %#v, want jq argv", cfg.Monitors[0].ResponseBody.Command)
	}
	if cfg.Monitors[0].ResponseBody.CommandTimeout.Duration != 2*time.Second {
		t.Fatalf("response_body.command_timeout = %s, want 2s", cfg.Monitors[0].ResponseBody.CommandTimeout.Duration)
	}
}

func TestParseDefaultsResponseBodyCommandTimeout(t *testing.T) {
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
      command: ["jq", "-e", ".ok == true"]
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Monitors[0].ResponseBody.CommandTimeout.Duration != 10*time.Second {
		t.Fatalf("response_body.command_timeout = %s, want 10s", cfg.Monitors[0].ResponseBody.CommandTimeout.Duration)
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

func TestParseRejectsMissingTelegramFields(t *testing.T) {
	_, err := Parse([]byte(`
alerts:
  providers:
    telegram:
      chat_ids: [""]
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
	for _, want := range []string{"alerts.providers.telegram.token is required", "alerts.providers.telegram.chat_ids[0] is required"} {
		if !strings.Contains(message, want) {
			t.Fatalf("validation error %q does not contain %q", message, want)
		}
	}
}

func TestParseRejectsDuplicateLegacyAndProviderConfig(t *testing.T) {
	_, err := Parse([]byte(`
smtp:
  host: smtp.example.com
  from: alerts@example.com
  to: [ops@example.com]
alerts:
  providers:
    smtp:
      host: smtp2.example.com
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
	if !strings.Contains(err.Error(), "smtp must be configured either at top level or alerts.providers.smtp, not both") {
		t.Fatalf("validation error %q does not contain duplicate smtp error", err)
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

func TestParseRejectsInvalidResponseBodyCommand(t *testing.T) {
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
    response_body:
      command: [""]
      command_timeout: -1s
`))
	if err == nil {
		t.Fatal("expected validation error")
	}
	message := err.Error()
	for _, want := range []string{"monitors[0].response_body.command[0] is required", "monitors[0].response_body.command_timeout must be positive"} {
		if !strings.Contains(message, want) {
			t.Fatalf("validation error %q does not contain %q", message, want)
		}
	}
}

func TestParseRejectsEmptyResponseBodyCommand(t *testing.T) {
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
    response_body:
      command: []
`))
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "monitors[0].response_body.command must contain at least one item") {
		t.Fatalf("validation error %q does not contain empty command", err)
	}
}

func TestParseRejectsResponseBodyCommandTimeoutWithoutCommand(t *testing.T) {
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
    response_body:
      command_timeout: 1s
`))
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "monitors[0].response_body.command_timeout requires response_body.command") {
		t.Fatalf("validation error %q does not contain command_timeout dependency", err)
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
