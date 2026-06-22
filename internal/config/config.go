package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
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
	Defaults Defaults        `yaml:"defaults"`
	Monitors []MonitorConfig `yaml:"monitors"`
}

type HTTPConfig struct {
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
}

type AlertsConfig struct {
	NotificationRetries NotificationRetriesConfig `yaml:"notification_retries"`
}

type NotificationRetriesConfig struct {
	MaxAttempts int        `yaml:"max_attempts"`
	Backoff     []Duration `yaml:"backoff"`
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
	ID                 string                 `yaml:"id"`
	Name               string                 `yaml:"name"`
	URL                string                 `yaml:"url"`
	ExpectedStatusCode int                    `yaml:"expected_status_code"`
	ResponseBody       ResponseBodyAssertions `yaml:"response_body"`
	MaxResponseTime    Duration               `yaml:"max_response_time"`
	Interval           Duration               `yaml:"interval"`
	Timeout            Duration               `yaml:"timeout"`
	InsecureSkipVerify bool                   `yaml:"insecure_skip_verify"`
}

type ResponseBodyAssertions struct {
	MustContain    string   `yaml:"must_contain"`
	MustNotContain string   `yaml:"must_not_contain"`
	Command        []string `yaml:"command"`
	CommandTimeout Duration `yaml:"command_timeout"`
}

func (a ResponseBodyAssertions) Configured() bool {
	return a.MustContain != "" || a.MustNotContain != "" || len(a.Command) != 0
}

type Duration struct {
	time.Duration
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
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
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
	if c.SMTP.Port == 0 {
		c.SMTP.Port = 587
	}
	if c.SMTP.TLS == "" {
		c.SMTP.TLS = "starttls"
	}
	if c.Mailtrap.Endpoint == "" {
		c.Mailtrap.Endpoint = "https://send.api.mailtrap.io/api/send"
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
		if c.Monitors[i].Interval.Duration == 0 {
			c.Monitors[i].Interval = c.Defaults.Interval
		}
		if c.Monitors[i].Timeout.Duration == 0 {
			c.Monitors[i].Timeout = c.Defaults.Timeout
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

func (c Config) Validate() error {
	var errs []error
	smtpConfigured := c.SMTP.IsConfigured()
	mailtrapConfigured := c.Mailtrap.IsConfigured()
	if !smtpConfigured && !mailtrapConfigured {
		errs = append(errs, errors.New("at least one alert provider must be configured"))
	}
	if smtpConfigured {
		if c.SMTP.Host == "" {
			errs = append(errs, errors.New("smtp.host is required"))
		}
		if c.SMTP.Port <= 0 || c.SMTP.Port > 65535 {
			errs = append(errs, errors.New("smtp.port must be a TCP port number from 1 through 65535"))
		}
		switch c.SMTP.TLS {
		case "none", "starttls", "tls":
		default:
			errs = append(errs, errors.New("smtp.tls must be one of: none, starttls, tls"))
		}
		if c.SMTP.From == "" {
			errs = append(errs, errors.New("smtp.from is required"))
		}
		if len(c.SMTP.To) == 0 {
			errs = append(errs, errors.New("smtp.to must contain at least one recipient"))
		}
	}
	if mailtrapConfigured {
		if c.Mailtrap.Token == "" {
			errs = append(errs, errors.New("mailtrap.token is required"))
		}
		if c.Mailtrap.Endpoint == "" {
			errs = append(errs, errors.New("mailtrap.endpoint is required"))
		} else if err := validateHTTPURL(c.Mailtrap.Endpoint); err != nil {
			errs = append(errs, fmt.Errorf("mailtrap.endpoint: %w", err))
		}
		if c.Mailtrap.From == "" {
			errs = append(errs, errors.New("mailtrap.from is required"))
		}
		if len(c.Mailtrap.To) == 0 {
			errs = append(errs, errors.New("mailtrap.to must contain at least one recipient"))
		}
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
	if c.HTTP.Port < 0 || c.HTTP.Port > 65535 {
		errs = append(errs, errors.New("http.port must be a TCP port number from 0 through 65535"))
	}
	if err := validateHTTPAddress(c.HTTP.Address); err != nil {
		errs = append(errs, fmt.Errorf("http.address: %w", err))
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
	}

	return errors.Join(errs...)
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
