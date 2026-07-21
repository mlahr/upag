package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	HTTP     HTTPConfig      `yaml:"http"`
	Alerts   AlertsConfig    `yaml:"alerts"`
	SMTP     SMTPConfig      `yaml:"smtp"`
	Mailtrap MailtrapConfig  `yaml:"mailtrap"`
	Observer ObserverConfig  `yaml:"observer"`
	Storage  StorageConfig   `yaml:"storage"`
	TenantID string          `yaml:"tenant_id"`
	Defaults Defaults        `yaml:"defaults"`
	Monitors []MonitorConfig `yaml:"monitors"`
}

type HTTPConfig struct {
	Address string         `yaml:"address"`
	Port    int            `yaml:"port"`
	Auth    HTTPAuthConfig `yaml:"auth"`
}

type HTTPAuthConfig struct {
	BearerToken string `yaml:"bearer_token"`
}

type AlertsConfig struct {
	NotificationRetries NotificationRetriesConfig `yaml:"notification_retries"`
	Providers           AlertProvidersConfig      `yaml:"providers"`
}

type NotificationRetriesConfig struct {
	MaxAttempts int        `yaml:"max_attempts"`
	Backoff     []Duration `yaml:"backoff"`
}

type AlertProvidersConfig struct {
	SMTP     SMTPConfig     `yaml:"smtp"`
	Mailtrap MailtrapConfig `yaml:"mailtrap"`
	Telegram TelegramConfig `yaml:"telegram"`
}

type SMTPConfig struct {
	Host      string   `yaml:"host"`
	Port      int      `yaml:"port"`
	TLS       string   `yaml:"tls"`
	Username  string   `yaml:"username"`
	Password  string   `yaml:"password"`
	From      string   `yaml:"from"`
	To        []string `yaml:"to"`
	LocalName string   `yaml:"local_name"`
}

type MailtrapConfig struct {
	Token    string   `yaml:"token"`
	Endpoint string   `yaml:"endpoint"`
	From     string   `yaml:"from"`
	FromName string   `yaml:"from_name"`
	To       []string `yaml:"to"`
}

type TelegramConfig struct {
	Token    string   `yaml:"token"`
	ChatIDs  []string `yaml:"chat_ids"`
	Endpoint string   `yaml:"endpoint"`
}

type ObserverConfig struct {
	Enabled           bool             `yaml:"enabled"`
	Interval          Duration         `yaml:"interval"`
	Timeout           Duration         `yaml:"timeout"`
	FailureThreshold  int              `yaml:"failure_threshold"`
	RecoveryThreshold int              `yaml:"recovery_threshold"`
	RequiredSuccesses int              `yaml:"required_successes"`
	Sentinels         []SentinelConfig `yaml:"sentinels"`
	enabledSet        bool
}

type StorageConfig struct {
	Backend               string          `yaml:"backend"`
	SQLite                SQLiteConfig    `yaml:"sqlite"`
	Postgres              PostgresConfig  `yaml:"postgres"`
	ProbeResults          RetentionConfig `yaml:"probe_results"`
	ProbeMinuteRollups    RetentionConfig `yaml:"probe_minute_rollups"`
	ProbeHourlyRollups    RetentionConfig `yaml:"probe_hourly_rollups"`
	ProbeDailyRollups     RetentionConfig `yaml:"probe_daily_rollups"`
	probeResultsSet       bool
	probeMinuteRollupsSet bool
	probeHourlyRollupsSet bool
	probeDailyRollupsSet  bool
}

type SQLiteConfig struct {
	Path string `yaml:"path"`
}

type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

type RetentionConfig struct {
	Retention RetentionDuration `yaml:"retention"`
}

type SentinelConfig struct {
	ID                 string `yaml:"id"`
	Name               string `yaml:"name"`
	URL                string `yaml:"url"`
	ExpectedStatusCode int    `yaml:"expected_status_code"`
}

type Defaults struct {
	Interval          Duration `yaml:"interval"`
	Timeout           Duration `yaml:"timeout"`
	ProbeRetries      int      `yaml:"probe_retries"`
	ProbeRetryBackoff Duration `yaml:"probe_retry_backoff"`
	FailureThreshold  int      `yaml:"failure_threshold"`
	HistoryRetention  Duration `yaml:"history_retention"`
	probeRetriesSet   bool
}

type MonitorConfig struct {
	ID                 string                   `yaml:"id"`
	Name               string                   `yaml:"name"`
	URL                string                   `yaml:"url"`
	ExpectedStatusCode int                      `yaml:"expected_status_code"`
	FollowRedirects    bool                     `yaml:"follow_redirects"`
	MaxRedirects       int                      `yaml:"max_redirects"`
	RedirectTarget     RedirectTargetAssertions `yaml:"redirect_target"`
	ResponseBody       ResponseBodyAssertions   `yaml:"response_body"`
	MaxResponseTime    Duration                 `yaml:"max_response_time"`
	Interval           Duration                 `yaml:"interval"`
	Timeout            Duration                 `yaml:"timeout"`
	FailureThreshold   int                      `yaml:"failure_threshold"`
	InsecureSkipVerify bool                     `yaml:"insecure_skip_verify"`
}

type RedirectTargetAssertions struct {
	Exact string `yaml:"exact"`
	Regex string `yaml:"regex"`
}

func (a RedirectTargetAssertions) Configured() bool {
	return a.Exact != "" || a.Regex != ""
}

type ResponseBodyAssertions struct {
	MustContain    []string `yaml:"must_contain"`
	MustNotContain []string `yaml:"must_not_contain"`
	Command        []string `yaml:"command"`
	CommandTimeout Duration `yaml:"command_timeout"`
}

const DefaultMaxRedirects = 10

func (a ResponseBodyAssertions) Configured() bool {
	return len(a.MustContain) > 0 || len(a.MustNotContain) > 0 || len(a.Command) != 0
}

type Duration struct {
	time.Duration
}

type RetentionDuration struct {
	Duration time.Duration
	Forever  bool
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var raw string
	if err := value.Decode(&raw); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

func (d *RetentionDuration) UnmarshalYAML(value *yaml.Node) error {
	var raw string
	if err := value.Decode(&raw); err != nil {
		return err
	}
	raw = strings.TrimSpace(raw)
	if raw == "forever" {
		d.Duration = 0
		d.Forever = true
		return nil
	}
	if strings.HasSuffix(raw, "d") || strings.HasSuffix(raw, "y") {
		unit := raw[len(raw)-1]
		number := strings.TrimSpace(raw[:len(raw)-1])
		parsed, err := time.ParseDuration(number + "h")
		if err != nil {
			return err
		}
		if unit == 'd' {
			d.Duration = parsed * 24
		} else {
			d.Duration = parsed * 24 * 365
		}
		d.Forever = false
		return nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return err
	}
	d.Duration = parsed
	d.Forever = false
	return nil
}

func (d *Defaults) UnmarshalYAML(value *yaml.Node) error {
	raw := struct {
		Interval          Duration `yaml:"interval"`
		Timeout           Duration `yaml:"timeout"`
		ProbeRetries      *int     `yaml:"probe_retries"`
		ProbeRetryBackoff Duration `yaml:"probe_retry_backoff"`
		FailureThreshold  int      `yaml:"failure_threshold"`
		HistoryRetention  Duration `yaml:"history_retention"`
	}{}
	if err := value.Decode(&raw); err != nil {
		return err
	}
	d.Interval = raw.Interval
	d.Timeout = raw.Timeout
	d.ProbeRetryBackoff = raw.ProbeRetryBackoff
	d.FailureThreshold = raw.FailureThreshold
	d.HistoryRetention = raw.HistoryRetention
	if raw.ProbeRetries != nil {
		d.ProbeRetries = *raw.ProbeRetries
		d.probeRetriesSet = true
	}
	return nil
}

func (s *StorageConfig) UnmarshalYAML(value *yaml.Node) error {
	raw := struct {
		Backend            string           `yaml:"backend"`
		SQLite             SQLiteConfig     `yaml:"sqlite"`
		Postgres           PostgresConfig   `yaml:"postgres"`
		ProbeResults       *RetentionConfig `yaml:"probe_results"`
		ProbeMinuteRollups *RetentionConfig `yaml:"probe_minute_rollups"`
		ProbeHourlyRollups *RetentionConfig `yaml:"probe_hourly_rollups"`
		ProbeDailyRollups  *RetentionConfig `yaml:"probe_daily_rollups"`
	}{}
	if err := value.Decode(&raw); err != nil {
		return err
	}
	s.Backend = raw.Backend
	s.SQLite = raw.SQLite
	s.Postgres = raw.Postgres
	if raw.ProbeResults != nil {
		s.ProbeResults = *raw.ProbeResults
		s.probeResultsSet = true
	}
	if raw.ProbeMinuteRollups != nil {
		s.ProbeMinuteRollups = *raw.ProbeMinuteRollups
		s.probeMinuteRollupsSet = true
	}
	if raw.ProbeHourlyRollups != nil {
		s.ProbeHourlyRollups = *raw.ProbeHourlyRollups
		s.probeHourlyRollupsSet = true
	}
	if raw.ProbeDailyRollups != nil {
		s.ProbeDailyRollups = *raw.ProbeDailyRollups
		s.probeDailyRollupsSet = true
	}
	return nil
}

func (o *ObserverConfig) UnmarshalYAML(value *yaml.Node) error {
	raw := struct {
		Enabled           *bool            `yaml:"enabled"`
		Interval          Duration         `yaml:"interval"`
		Timeout           Duration         `yaml:"timeout"`
		FailureThreshold  int              `yaml:"failure_threshold"`
		RecoveryThreshold int              `yaml:"recovery_threshold"`
		RequiredSuccesses int              `yaml:"required_successes"`
		Sentinels         []SentinelConfig `yaml:"sentinels"`
	}{}
	if err := value.Decode(&raw); err != nil {
		return err
	}
	if raw.Enabled != nil {
		o.Enabled = *raw.Enabled
		o.enabledSet = true
	}
	o.Interval = raw.Interval
	o.Timeout = raw.Timeout
	o.FailureThreshold = raw.FailureThreshold
	o.RecoveryThreshold = raw.RecoveryThreshold
	o.RequiredSuccesses = raw.RequiredSuccesses
	o.Sentinels = raw.Sentinels
	return nil
}

func LoadFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	return Parse(data)
}

func Parse(data []byte) (Config, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if err := validateConfigSchema(&root); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	var cfg Config
	if err := root.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

type schemaValidator func(*yaml.Node, string) error

func validateConfigSchema(root *yaml.Node) error {
	if root.Kind == yaml.DocumentNode {
		if len(root.Content) == 0 {
			return nil
		}
		root = root.Content[0]
	}
	return validateMapping(root, "", map[string]schemaValidator{
		"http":      validateHTTPConfigSchema,
		"alerts":    validateAlertsConfigSchema,
		"smtp":      validateSMTPConfigSchema,
		"mailtrap":  validateMailtrapConfigSchema,
		"observer":  validateObserverConfigSchema,
		"storage":   validateStorageConfigSchema,
		"tenant_id": nil,
		"defaults":  validateDefaultsSchema,
		"monitors":  validateMonitorsSchema,
	})
}

func validateMapping(node *yaml.Node, path string, fields map[string]schemaValidator) error {
	if node.Kind != yaml.MappingNode {
		return nil
	}
	seen := map[string]struct{}{}
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		value := node.Content[i+1]
		if key.Kind != yaml.ScalarNode {
			return fmt.Errorf("config field %s must be a string key", formatPath(path))
		}
		fieldPath := joinPath(path, key.Value)
		validator, ok := fields[key.Value]
		if !ok {
			return fmt.Errorf("unknown config field %s", fieldPath)
		}
		if _, ok := seen[key.Value]; ok {
			return fmt.Errorf("duplicate config field %s", fieldPath)
		}
		seen[key.Value] = struct{}{}
		if validator != nil {
			if err := validator(value, fieldPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func formatPath(path string) string {
	if path == "" {
		return "<root>"
	}
	return path
}

func joinPath(path, field string) string {
	if path == "" {
		return field
	}
	return path + "." + field
}

func validateHTTPConfigSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"address": nil,
		"port":    nil,
		"auth":    validateHTTPAuthConfigSchema,
	})
}

func validateHTTPAuthConfigSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"bearer_token": nil,
	})
}

func validateAlertsConfigSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"notification_retries": validateNotificationRetriesSchema,
		"providers":            validateAlertProvidersSchema,
	})
}

func validateNotificationRetriesSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"max_attempts": nil,
		"backoff":      nil,
	})
}

func validateAlertProvidersSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"smtp":     validateSMTPConfigSchema,
		"mailtrap": validateMailtrapConfigSchema,
		"telegram": validateTelegramConfigSchema,
	})
}

func validateSMTPConfigSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"host":       nil,
		"port":       nil,
		"tls":        nil,
		"username":   nil,
		"password":   nil,
		"from":       nil,
		"to":         nil,
		"local_name": nil,
	})
}

func validateMailtrapConfigSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"token":     nil,
		"endpoint":  nil,
		"from":      nil,
		"from_name": nil,
		"to":        nil,
	})
}

func validateTelegramConfigSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"token":    nil,
		"chat_ids": nil,
		"endpoint": nil,
	})
}

func validateObserverConfigSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"enabled":            nil,
		"interval":           nil,
		"timeout":            nil,
		"failure_threshold":  nil,
		"recovery_threshold": nil,
		"required_successes": nil,
		"sentinels":          validateSentinelsSchema,
	})
}

func validateSentinelsSchema(node *yaml.Node, path string) error {
	if node.Kind != yaml.SequenceNode {
		return nil
	}
	for i, item := range node.Content {
		itemPath := fmt.Sprintf("%s[%d]", path, i)
		if err := validateMapping(item, itemPath, map[string]schemaValidator{
			"id":                   nil,
			"name":                 nil,
			"url":                  nil,
			"expected_status_code": nil,
		}); err != nil {
			return err
		}
	}
	return nil
}

func validatePositiveInteger(node *yaml.Node, path string) error {
	var value int
	if err := node.Decode(&value); err != nil {
		return nil
	}
	if value <= 0 {
		return fmt.Errorf("config field %s must be positive", path)
	}
	return nil
}

func validateRedirectTargetSchema(node *yaml.Node, path string) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("config field %s must be a mapping", path)
	}
	if len(node.Content) == 0 {
		return fmt.Errorf("config field %s must configure exactly one of exact or regex", path)
	}
	return validateMapping(node, path, map[string]schemaValidator{
		"exact": validateNonEmptyString,
		"regex": validateNonEmptyString,
	})
}

func validateNonEmptyString(node *yaml.Node, path string) error {
	var value string
	if err := node.Decode(&value); err != nil {
		return nil
	}
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("config field %s must not be empty", path)
	}
	return nil
}

func validateStorageConfigSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"backend":              nil,
		"sqlite":               validateSQLiteConfigSchema,
		"postgres":             validatePostgresConfigSchema,
		"probe_results":        validateRetentionConfigSchema,
		"probe_minute_rollups": validateRetentionConfigSchema,
		"probe_hourly_rollups": validateRetentionConfigSchema,
		"probe_daily_rollups":  validateRetentionConfigSchema,
	})
}

func validateSQLiteConfigSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"path": nil,
	})
}

func validatePostgresConfigSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"dsn": nil,
	})
}

func validateRetentionConfigSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"retention": nil,
	})
}

func validateDefaultsSchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"interval":            nil,
		"timeout":             nil,
		"probe_retries":       nil,
		"probe_retry_backoff": nil,
		"failure_threshold":   nil,
		"history_retention":   nil,
	})
}

func validateMonitorsSchema(node *yaml.Node, path string) error {
	if node.Kind != yaml.SequenceNode {
		return nil
	}
	for i, item := range node.Content {
		itemPath := fmt.Sprintf("%s[%d]", path, i)
		if err := validateMapping(item, itemPath, map[string]schemaValidator{
			"id":                   nil,
			"name":                 nil,
			"url":                  nil,
			"expected_status_code": nil,
			"follow_redirects":     nil,
			"max_redirects":        validatePositiveInteger,
			"redirect_target":      validateRedirectTargetSchema,
			"response_body":        validateResponseBodySchema,
			"max_response_time":    nil,
			"interval":             nil,
			"timeout":              nil,
			"failure_threshold":    nil,
			"insecure_skip_verify": nil,
		}); err != nil {
			return err
		}
	}
	return nil
}

func validateResponseBodySchema(node *yaml.Node, path string) error {
	return validateMapping(node, path, map[string]schemaValidator{
		"must_contain":     nil,
		"must_not_contain": nil,
		"command":          nil,
		"command_timeout":  nil,
	})
}

func (c *Config) ApplyDefaults() {
	if c.HTTP.Address == "" {
		c.HTTP.Address = "127.0.0.1"
	}
	if c.Alerts.NotificationRetries.MaxAttempts == 0 {
		c.Alerts.NotificationRetries.MaxAttempts = 3
	}
	if len(c.Alerts.NotificationRetries.Backoff) == 0 {
		c.Alerts.NotificationRetries.Backoff = []Duration{
			{Duration: time.Minute},
			{Duration: 5 * time.Minute},
			{Duration: 15 * time.Minute},
		}
	}
	if c.Defaults.Interval.Duration == 0 {
		c.Defaults.Interval.Duration = time.Minute
	}
	if c.Defaults.Timeout.Duration == 0 {
		c.Defaults.Timeout.Duration = 10 * time.Second
	}
	if !c.Defaults.probeRetriesSet {
		c.Defaults.ProbeRetries = 2
	}
	if c.Defaults.ProbeRetryBackoff.Duration == 0 {
		c.Defaults.ProbeRetryBackoff.Duration = 500 * time.Millisecond
	}
	if c.Defaults.FailureThreshold == 0 {
		c.Defaults.FailureThreshold = 3
	}
	if c.Defaults.HistoryRetention.Duration == 0 {
		c.Defaults.HistoryRetention.Duration = 30 * 24 * time.Hour
	}
	if c.Storage.Backend == "" {
		c.Storage.Backend = "sqlite"
	}
	if c.Storage.Backend == "sqlite" && c.Storage.SQLite.Path == "" {
		c.Storage.SQLite.Path = "./upag.sqlite"
	}
	if !c.Storage.probeResultsSet {
		c.Storage.ProbeResults.Retention.Duration = c.Defaults.HistoryRetention.Duration
	}
	if !c.Storage.probeMinuteRollupsSet {
		c.Storage.ProbeMinuteRollups.Retention.Duration = 30 * 24 * time.Hour
	}
	if !c.Storage.probeHourlyRollupsSet {
		c.Storage.ProbeHourlyRollups.Retention.Duration = 365 * 24 * time.Hour
	}
	if !c.Storage.probeDailyRollupsSet {
		c.Storage.ProbeDailyRollups.Retention.Forever = true
	}
	if c.TenantID == "" {
		c.TenantID = "default"
	}
	if c.SMTP.Port == 0 {
		c.SMTP.Port = 587
	}
	if c.SMTP.TLS == "" {
		c.SMTP.TLS = "starttls"
	}
	if c.Alerts.Providers.SMTP.Port == 0 {
		c.Alerts.Providers.SMTP.Port = 587
	}
	if c.Alerts.Providers.SMTP.TLS == "" {
		c.Alerts.Providers.SMTP.TLS = "starttls"
	}
	if c.Mailtrap.Endpoint == "" {
		c.Mailtrap.Endpoint = "https://send.api.mailtrap.io/api/send"
	}
	if c.Alerts.Providers.Mailtrap.Endpoint == "" {
		c.Alerts.Providers.Mailtrap.Endpoint = "https://send.api.mailtrap.io/api/send"
	}
	if c.Alerts.Providers.Telegram.Endpoint == "" {
		c.Alerts.Providers.Telegram.Endpoint = "https://api.telegram.org"
	}
	if !c.Observer.enabledSet {
		c.Observer.Enabled = true
	}
	if c.Observer.Interval.Duration == 0 {
		c.Observer.Interval.Duration = 30 * time.Second
	}
	if c.Observer.Timeout.Duration == 0 {
		c.Observer.Timeout.Duration = 5 * time.Second
	}
	if c.Observer.FailureThreshold == 0 {
		c.Observer.FailureThreshold = 3
	}
	if c.Observer.RecoveryThreshold == 0 {
		c.Observer.RecoveryThreshold = 1
	}
	if c.Observer.RequiredSuccesses == 0 {
		c.Observer.RequiredSuccesses = 1
	}
	if c.Observer.Enabled && len(c.Observer.Sentinels) == 0 {
		c.Observer.Sentinels = BuiltInObserverSentinels()
	}
	for i := range c.Monitors {
		if c.Monitors[i].FollowRedirects && c.Monitors[i].MaxRedirects == 0 {
			c.Monitors[i].MaxRedirects = DefaultMaxRedirects
		}
		if c.Monitors[i].Interval.Duration == 0 {
			c.Monitors[i].Interval = c.Defaults.Interval
		}
		if c.Monitors[i].Timeout.Duration == 0 {
			c.Monitors[i].Timeout = c.Defaults.Timeout
		}
		if c.Monitors[i].FailureThreshold == 0 {
			c.Monitors[i].FailureThreshold = c.Defaults.FailureThreshold
		}
		if len(c.Monitors[i].ResponseBody.Command) != 0 && c.Monitors[i].ResponseBody.CommandTimeout.Duration == 0 {
			c.Monitors[i].ResponseBody.CommandTimeout.Duration = 10 * time.Second
		}
	}
}

func BuiltInObserverSentinels() []SentinelConfig {
	return []SentinelConfig{
		{
			ID:                 "gstatic",
			Name:               "Google connectivity check",
			URL:                "https://www.gstatic.com/generate_204",
			ExpectedStatusCode: 204,
		},
		{
			ID:                 "cloudflare",
			Name:               "Cloudflare connectivity check",
			URL:                "https://cp.cloudflare.com/generate_204",
			ExpectedStatusCode: 204,
		},
		{
			ID:                 "cloudflare-ip",
			Name:               "Cloudflare IPv4 connectivity check",
			URL:                "http://1.1.1.1/cdn-cgi/trace",
			ExpectedStatusCode: 301,
		},
		{
			ID:                 "msftconnecttest",
			Name:               "Microsoft connectivity check",
			URL:                "http://www.msftconnecttest.com/connecttest.txt",
			ExpectedStatusCode: 200,
		},
	}
}

func (c SMTPConfig) IsConfigured() bool {
	return c.Host != "" ||
		c.Username != "" ||
		c.Password != "" ||
		c.From != "" ||
		len(c.To) != 0 ||
		c.LocalName != ""
}

func (c MailtrapConfig) IsConfigured() bool {
	return c.Token != "" ||
		c.From != "" ||
		c.FromName != "" ||
		len(c.To) != 0
}

func (c TelegramConfig) IsConfigured() bool {
	return c.Token != "" ||
		len(c.ChatIDs) != 0
}

func validateSMTPConfig(path string, c SMTPConfig, errs *[]error) {
	if c.Host == "" {
		*errs = append(*errs, fmt.Errorf("%s.host is required", path))
	}
	if c.Port <= 0 || c.Port > 65535 {
		*errs = append(*errs, fmt.Errorf("%s.port must be a TCP port number from 1 through 65535", path))
	}
	switch c.TLS {
	case "none", "starttls", "tls":
	default:
		*errs = append(*errs, fmt.Errorf("%s.tls must be one of: none, starttls, tls", path))
	}
	if c.From == "" {
		*errs = append(*errs, fmt.Errorf("%s.from is required", path))
	}
	if len(c.To) == 0 {
		*errs = append(*errs, fmt.Errorf("%s.to must contain at least one recipient", path))
	}
}

func validateMailtrapConfig(path string, c MailtrapConfig, errs *[]error) {
	if c.Token == "" {
		*errs = append(*errs, fmt.Errorf("%s.token is required", path))
	}
	if c.Endpoint == "" {
		*errs = append(*errs, fmt.Errorf("%s.endpoint is required", path))
	} else if err := validateHTTPURL(c.Endpoint); err != nil {
		*errs = append(*errs, fmt.Errorf("%s.endpoint: %w", path, err))
	}
	if c.From == "" {
		*errs = append(*errs, fmt.Errorf("%s.from is required", path))
	}
	if len(c.To) == 0 {
		*errs = append(*errs, fmt.Errorf("%s.to must contain at least one recipient", path))
	}
}

func validateTelegramConfig(path string, c TelegramConfig, errs *[]error) {
	if c.Token == "" {
		*errs = append(*errs, fmt.Errorf("%s.token is required", path))
	}
	if len(c.ChatIDs) == 0 {
		*errs = append(*errs, fmt.Errorf("%s.chat_ids must contain at least one chat ID", path))
	}
	for i, chatID := range c.ChatIDs {
		if strings.TrimSpace(chatID) == "" {
			*errs = append(*errs, fmt.Errorf("%s.chat_ids[%d] is required", path, i))
		}
	}
	if c.Endpoint == "" {
		*errs = append(*errs, fmt.Errorf("%s.endpoint is required", path))
	} else if err := validateHTTPURL(c.Endpoint); err != nil {
		*errs = append(*errs, fmt.Errorf("%s.endpoint: %w", path, err))
	}
}

func (c Config) Validate() error {
	var errs []error
	smtpConfigured := c.SMTP.IsConfigured()
	mailtrapConfigured := c.Mailtrap.IsConfigured()
	providerSMTPConfigured := c.Alerts.Providers.SMTP.IsConfigured()
	providerMailtrapConfigured := c.Alerts.Providers.Mailtrap.IsConfigured()
	telegramConfigured := c.Alerts.Providers.Telegram.IsConfigured()
	if !smtpConfigured && !mailtrapConfigured && !providerSMTPConfigured && !providerMailtrapConfigured && !telegramConfigured {
		errs = append(errs, errors.New("at least one alert provider must be configured"))
	}
	if smtpConfigured && providerSMTPConfigured {
		errs = append(errs, errors.New("smtp must be configured either at top level or alerts.providers.smtp, not both"))
	}
	if mailtrapConfigured && providerMailtrapConfigured {
		errs = append(errs, errors.New("mailtrap must be configured either at top level or alerts.providers.mailtrap, not both"))
	}
	if smtpConfigured {
		validateSMTPConfig("smtp", c.SMTP, &errs)
	}
	if providerSMTPConfigured {
		validateSMTPConfig("alerts.providers.smtp", c.Alerts.Providers.SMTP, &errs)
	}
	if mailtrapConfigured {
		validateMailtrapConfig("mailtrap", c.Mailtrap, &errs)
	}
	if providerMailtrapConfigured {
		validateMailtrapConfig("alerts.providers.mailtrap", c.Alerts.Providers.Mailtrap, &errs)
	}
	if telegramConfigured {
		validateTelegramConfig("alerts.providers.telegram", c.Alerts.Providers.Telegram, &errs)
	}
	if c.Defaults.Interval.Duration <= 0 {
		errs = append(errs, errors.New("defaults.interval must be positive"))
	}
	if c.Defaults.Timeout.Duration <= 0 {
		errs = append(errs, errors.New("defaults.timeout must be positive"))
	}
	if c.Defaults.ProbeRetries < 0 {
		errs = append(errs, errors.New("defaults.probe_retries must be non-negative"))
	}
	if c.Defaults.ProbeRetries > 0 && c.Defaults.ProbeRetryBackoff.Duration <= 0 {
		errs = append(errs, errors.New("defaults.probe_retry_backoff must be positive when defaults.probe_retries is positive"))
	}
	if c.Defaults.FailureThreshold <= 0 {
		errs = append(errs, errors.New("defaults.failure_threshold must be positive"))
	}
	if c.Defaults.HistoryRetention.Duration <= 0 {
		errs = append(errs, errors.New("defaults.history_retention must be positive"))
	}
	validateRetention := func(path string, retention RetentionDuration) {
		if !retention.Forever && retention.Duration <= 0 {
			errs = append(errs, fmt.Errorf("%s.retention must be positive or forever", path))
		}
	}
	validateRetention("storage.probe_results", c.Storage.ProbeResults.Retention)
	validateRetention("storage.probe_minute_rollups", c.Storage.ProbeMinuteRollups.Retention)
	validateRetention("storage.probe_hourly_rollups", c.Storage.ProbeHourlyRollups.Retention)
	validateRetention("storage.probe_daily_rollups", c.Storage.ProbeDailyRollups.Retention)
	switch c.Storage.Backend {
	case "sqlite":
		if strings.TrimSpace(c.Storage.SQLite.Path) == "" {
			errs = append(errs, errors.New("storage.sqlite.path is required when storage.backend is sqlite"))
		}
	case "postgres":
		if strings.TrimSpace(c.Storage.Postgres.DSN) == "" {
			errs = append(errs, errors.New("storage.postgres.dsn is required when storage.backend is postgres"))
		}
	default:
		errs = append(errs, errors.New("storage.backend must be one of: sqlite, postgres"))
	}
	if c.HTTP.Port < 0 || c.HTTP.Port > 65535 {
		errs = append(errs, errors.New("http.port must be a TCP port number from 0 through 65535"))
	}
	if err := validateTenantID(c.TenantID); err != nil {
		errs = append(errs, err)
	}
	if err := validateHTTPAddress(c.HTTP.Address); err != nil {
		errs = append(errs, fmt.Errorf("http.address: %w", err))
	}
	if token := c.HTTP.Auth.BearerToken; token != "" {
		if strings.TrimSpace(token) != token {
			errs = append(errs, errors.New("http.auth.bearer_token must not have leading or trailing whitespace"))
		}
		for _, r := range token {
			if r <= 0x20 || r == 0x7f {
				errs = append(errs, errors.New("http.auth.bearer_token must not contain whitespace or control characters"))
				break
			}
		}
	}
	if c.Observer.Interval.Duration <= 0 {
		errs = append(errs, errors.New("observer.interval must be positive"))
	}
	if c.Observer.Timeout.Duration <= 0 {
		errs = append(errs, errors.New("observer.timeout must be positive"))
	}
	if c.Observer.FailureThreshold <= 0 {
		errs = append(errs, errors.New("observer.failure_threshold must be positive"))
	}
	if c.Observer.RecoveryThreshold <= 0 {
		errs = append(errs, errors.New("observer.recovery_threshold must be positive"))
	}
	if c.Observer.RequiredSuccesses <= 0 {
		errs = append(errs, errors.New("observer.required_successes must be positive"))
	}
	if c.Observer.Enabled {
		if len(c.Observer.Sentinels) == 0 {
			errs = append(errs, errors.New("observer.sentinels must contain at least one sentinel when observer is enabled"))
		}
		if c.Observer.RequiredSuccesses > len(c.Observer.Sentinels) {
			errs = append(errs, errors.New("observer.required_successes must be less than or equal to the number of observer.sentinels"))
		}
	}
	sentinelIDs := map[string]struct{}{}
	for i, sentinel := range c.Observer.Sentinels {
		prefix := fmt.Sprintf("observer.sentinels[%d]", i)
		if sentinel.ID == "" {
			errs = append(errs, fmt.Errorf("%s.id is required", prefix))
		}
		if _, ok := sentinelIDs[sentinel.ID]; sentinel.ID != "" && ok {
			errs = append(errs, fmt.Errorf("%s.id %q is duplicated", prefix, sentinel.ID))
		}
		sentinelIDs[sentinel.ID] = struct{}{}
		if sentinel.Name == "" {
			errs = append(errs, fmt.Errorf("%s.name is required", prefix))
		}
		if sentinel.URL == "" {
			errs = append(errs, fmt.Errorf("%s.url is required", prefix))
		} else if err := validateHTTPURL(sentinel.URL); err != nil {
			errs = append(errs, fmt.Errorf("%s.url: %w", prefix, err))
		}
		if sentinel.ExpectedStatusCode < 100 || sentinel.ExpectedStatusCode > 599 {
			errs = append(errs, fmt.Errorf("%s.expected_status_code must be from 100 through 599", prefix))
		}
	}
	if len(c.Monitors) == 0 {
		errs = append(errs, errors.New("monitors must contain at least one monitor"))
	}
	if c.Alerts.NotificationRetries.MaxAttempts <= 0 {
		errs = append(errs, errors.New("alerts.notification_retries.max_attempts must be positive"))
	}
	if len(c.Alerts.NotificationRetries.Backoff) == 0 {
		errs = append(errs, errors.New("alerts.notification_retries.backoff must contain at least one duration"))
	}
	for i, backoff := range c.Alerts.NotificationRetries.Backoff {
		if backoff.Duration <= 0 {
			errs = append(errs, fmt.Errorf("alerts.notification_retries.backoff[%d] must be positive", i))
		}
	}

	ids := map[string]struct{}{}
	for i, monitor := range c.Monitors {
		prefix := fmt.Sprintf("monitors[%d]", i)
		if monitor.ID == "" {
			errs = append(errs, fmt.Errorf("%s.id is required", prefix))
		}
		if _, ok := ids[monitor.ID]; monitor.ID != "" && ok {
			errs = append(errs, fmt.Errorf("%s.id %q is duplicated", prefix, monitor.ID))
		}
		ids[monitor.ID] = struct{}{}
		if monitor.Name == "" {
			errs = append(errs, fmt.Errorf("%s.name is required", prefix))
		}
		if monitor.URL == "" {
			errs = append(errs, fmt.Errorf("%s.url is required", prefix))
		} else if err := validateHTTPURL(monitor.URL); err != nil {
			errs = append(errs, fmt.Errorf("%s.url: %w", prefix, err))
		}
		if monitor.ExpectedStatusCode < 100 || monitor.ExpectedStatusCode > 599 {
			errs = append(errs, fmt.Errorf("%s.expected_status_code must be from 100 through 599", prefix))
		}
		if monitor.MaxResponseTime.Duration < 0 {
			errs = append(errs, fmt.Errorf("%s.max_response_time must be positive", prefix))
		}
		if monitor.MaxRedirects < 0 {
			errs = append(errs, fmt.Errorf("%s.max_redirects must be positive", prefix))
		}
		if !monitor.FollowRedirects && monitor.MaxRedirects > 0 {
			errs = append(errs, fmt.Errorf("%s.max_redirects requires follow_redirects", prefix))
		}
		if !monitor.FollowRedirects && monitor.RedirectTarget.Configured() {
			errs = append(errs, fmt.Errorf("%s.redirect_target requires follow_redirects", prefix))
		}
		if monitor.RedirectTarget.Exact != "" && monitor.RedirectTarget.Regex != "" {
			errs = append(errs, fmt.Errorf("%s.redirect_target must configure exactly one of exact or regex", prefix))
		}
		if monitor.RedirectTarget.Exact != "" {
			if err := validateHTTPURL(monitor.RedirectTarget.Exact); err != nil {
				errs = append(errs, fmt.Errorf("%s.redirect_target.exact: %w", prefix, err))
			}
		}
		if monitor.RedirectTarget.Regex != "" {
			if _, err := regexp.Compile(fullURLRegexPattern(monitor.RedirectTarget.Regex)); err != nil {
				errs = append(errs, fmt.Errorf("%s.redirect_target.regex: invalid regular expression: %w", prefix, err))
			}
		}
		if monitor.ResponseBody.Command != nil && len(monitor.ResponseBody.Command) == 0 {
			errs = append(errs, fmt.Errorf("%s.response_body.command must contain at least one item", prefix))
		}
		if len(monitor.ResponseBody.Command) == 0 {
			if monitor.ResponseBody.CommandTimeout.Duration != 0 {
				errs = append(errs, fmt.Errorf("%s.response_body.command_timeout requires response_body.command", prefix))
			}
		} else {
			if strings.TrimSpace(monitor.ResponseBody.Command[0]) == "" {
				errs = append(errs, fmt.Errorf("%s.response_body.command[0] is required", prefix))
			}
			if monitor.ResponseBody.CommandTimeout.Duration <= 0 {
				errs = append(errs, fmt.Errorf("%s.response_body.command_timeout must be positive", prefix))
			}
		}
		if monitor.Interval.Duration <= 0 {
			errs = append(errs, fmt.Errorf("%s.interval must be positive", prefix))
		}
		if monitor.Timeout.Duration <= 0 {
			errs = append(errs, fmt.Errorf("%s.timeout must be positive", prefix))
		}
		if monitor.FailureThreshold <= 0 {
			errs = append(errs, fmt.Errorf("%s.failure_threshold must be positive", prefix))
		}
	}

	return errors.Join(errs...)
}

func fullURLRegexPattern(pattern string) string {
	return `\A(?:` + pattern + `)\z`
}

func validateHTTPURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("scheme must be http or https")
	}
	if parsed.Host == "" {
		return errors.New("host is required")
	}
	if strings.Contains(parsed.Host, " ") {
		return errors.New("host must not contain spaces")
	}
	return nil
}

func validateHTTPAddress(raw string) error {
	if raw == "" {
		return errors.New("address is required")
	}
	if raw != strings.TrimSpace(raw) {
		return errors.New("address must not contain leading or trailing whitespace")
	}
	if strings.ContainsAny(raw, " \t\r\n") {
		return errors.New("address must not contain whitespace")
	}
	if _, _, err := net.SplitHostPort(raw); err == nil {
		return errors.New("address must not include a port")
	}
	return nil
}

const maxTenantIDLength = 63

var tenantIDPattern = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9._-]{0,61}[a-zA-Z0-9])?$`)

func validateTenantID(raw string) error {
	tenantID := strings.TrimSpace(raw)
	if tenantID == "" {
		return errors.New("tenant_id is required")
	}
	if raw != tenantID {
		return errors.New("tenant_id must not have leading or trailing whitespace")
	}
	if len(tenantID) > maxTenantIDLength {
		return fmt.Errorf("tenant_id must not exceed %d characters", maxTenantIDLength)
	}
	if !tenantIDPattern.MatchString(tenantID) {
		return fmt.Errorf("tenant_id must match %s", tenantIDPattern.String())
	}
	return nil
}
