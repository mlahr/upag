package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Alerts   AlertsConfig    `yaml:"alerts"`
	SMTP     SMTPConfig      `yaml:"smtp"`
	Mailtrap MailtrapConfig  `yaml:"mailtrap"`
	Defaults Defaults        `yaml:"defaults"`
	Monitors []MonitorConfig `yaml:"monitors"`
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

type Defaults struct {
	Interval         Duration `yaml:"interval"`
	Timeout          Duration `yaml:"timeout"`
	FailureThreshold int      `yaml:"failure_threshold"`
	HistoryRetention Duration `yaml:"history_retention"`
}

type MonitorConfig struct {
	ID                 string   `yaml:"id"`
	Name               string   `yaml:"name"`
	URL                string   `yaml:"url"`
	ExpectedStatusCode int      `yaml:"expected_status_code"`
	Interval           Duration `yaml:"interval"`
	Timeout            Duration `yaml:"timeout"`
	InsecureSkipVerify bool     `yaml:"insecure_skip_verify"`
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
	for i := range c.Monitors {
		if c.Monitors[i].Interval.Duration == 0 {
			c.Monitors[i].Interval = c.Defaults.Interval
		}
		if c.Monitors[i].Timeout.Duration == 0 {
			c.Monitors[i].Timeout = c.Defaults.Timeout
		}
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
	if c.Defaults.FailureThreshold <= 0 {
		errs = append(errs, errors.New("defaults.failure_threshold must be positive"))
	}
	if c.Defaults.HistoryRetention.Duration <= 0 {
		errs = append(errs, errors.New("defaults.history_retention must be positive"))
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
