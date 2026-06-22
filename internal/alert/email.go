package alert

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/smtp"
	"strings"
	"time"

	"upag/internal/config"
	"upag/internal/observer"
	"upag/internal/storage"
)

type IncidentSender interface {
	SendIncident(incident storage.Incident, current storage.MonitorState) []SendResult
	SendProvider(provider string, incident storage.Incident, current storage.MonitorState) SendResult
	Providers() []string
}

type SendResult struct {
	Provider string
	Error    error
}

func SendResultsError(results []SendResult) error {
	var errs []error
	for _, result := range results {
		if result.Error != nil {
			errs = append(errs, fmt.Errorf("%s: %w", result.Provider, result.Error))
		}
	}
	return errors.Join(errs...)
}

func NewIncidentSender(cfg config.Config) IncidentSender {
	var senders []IncidentSender
	if cfg.SMTP.IsConfigured() {
		senders = append(senders, NewEmailer(cfg.SMTP))
	}
	if cfg.Mailtrap.IsConfigured() {
		senders = append(senders, NewMailtrapEmailer(cfg.Mailtrap))
	}
	return MultiSender{senders: senders}
}

type MultiSender struct {
	senders []IncidentSender
}

func (s MultiSender) SendIncident(incident storage.Incident, current storage.MonitorState) []SendResult {
	var results []SendResult
	for _, sender := range s.senders {
		results = append(results, sender.SendIncident(incident, current)...)
	}
	return results
}

func (s MultiSender) SendProvider(provider string, incident storage.Incident, current storage.MonitorState) SendResult {
	for _, sender := range s.senders {
		for _, senderProvider := range sender.Providers() {
			if senderProvider == provider {
				return sender.SendProvider(provider, incident, current)
			}
		}
	}
	return SendResult{Provider: provider, Error: fmt.Errorf("alert provider %q is not configured", provider)}
}

func (s MultiSender) Providers() []string {
	providers := make([]string, 0, len(s.senders))
	for _, sender := range s.senders {
		providers = append(providers, sender.Providers()...)
	}
	return providers
}

type Emailer struct {
	cfg config.SMTPConfig
}

func NewEmailer(cfg config.SMTPConfig) *Emailer {
	return &Emailer{cfg: cfg}
}

func (e *Emailer) Providers() []string {
	return []string{"smtp"}
}

func (e *Emailer) SendIncident(incident storage.Incident, current storage.MonitorState) []SendResult {
	return []SendResult{e.SendProvider("smtp", incident, current)}
}

func (e *Emailer) SendProvider(provider string, incident storage.Incident, current storage.MonitorState) SendResult {
	if provider != "smtp" {
		return SendResult{Provider: provider, Error: fmt.Errorf("smtp sender cannot send provider %q", provider)}
	}
	subject, body := buildIncidentContent(incident, current)
	message := buildMessage(e.cfg.From, e.cfg.To, subject, body)
	addr := net.JoinHostPort(e.cfg.Host, fmt.Sprintf("%d", e.cfg.Port))
	auth := smtp.Auth(nil)
	if e.cfg.Username != "" || e.cfg.Password != "" {
		host := e.cfg.Host
		auth = smtp.PlainAuth("", e.cfg.Username, e.cfg.Password, host)
	}

	switch e.cfg.TLS {
	case "tls":
		return SendResult{Provider: "smtp", Error: e.sendTLS(addr, auth, message)}
	case "starttls":
		return SendResult{Provider: "smtp", Error: e.sendStartTLS(addr, auth, message)}
	case "none":
		return SendResult{Provider: "smtp", Error: smtp.SendMail(addr, auth, e.cfg.From, e.cfg.To, message)}
	default:
		return SendResult{Provider: "smtp", Error: fmt.Errorf("unsupported smtp.tls mode %q", e.cfg.TLS)}
	}
}

func (e *Emailer) sendTLS(addr string, auth smtp.Auth, message []byte) error {
	conn, err := tls.Dial("tcp", addr, &tls.Config{ServerName: e.cfg.Host})
	if err != nil {
		return err
	}
	defer conn.Close()

	client, err := smtp.NewClient(conn, e.cfg.Host)
	if err != nil {
		return err
	}
	defer client.Quit()
	return e.sendWithClient(client, auth, message)
}

func (e *Emailer) sendStartTLS(addr string, auth smtp.Auth, message []byte) error {
	client, err := smtp.Dial(addr)
	if err != nil {
		return err
	}
	defer client.Quit()

	if ok, _ := client.Extension("STARTTLS"); ok {
		if err := client.StartTLS(&tls.Config{ServerName: e.cfg.Host}); err != nil {
			return err
		}
	}
	return e.sendWithClient(client, auth, message)
}

func (e *Emailer) sendWithClient(client *smtp.Client, auth smtp.Auth, message []byte) error {
	if e.cfg.LocalName != "" {
		if err := client.Hello(e.cfg.LocalName); err != nil {
			return err
		}
	}
	if auth != nil {
		if ok, _ := client.Extension("AUTH"); ok {
			if err := client.Auth(auth); err != nil {
				return err
			}
		}
	}
	if err := client.Mail(e.cfg.From); err != nil {
		return err
	}
	for _, recipient := range e.cfg.To {
		if err := client.Rcpt(recipient); err != nil {
			return err
		}
	}
	writer, err := client.Data()
	if err != nil {
		return err
	}
	if _, err := writer.Write(message); err != nil {
		writer.Close()
		return err
	}
	return writer.Close()
}

func buildMessage(from string, to []string, subject string, body string) []byte {
	headers := []string{
		fmt.Sprintf("From: %s", from),
		fmt.Sprintf("To: %s", strings.Join(to, ", ")),
		fmt.Sprintf("Subject: %s", subject),
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=utf-8",
		"",
		body,
	}
	return []byte(strings.Join(headers, "\r\n"))
}

type MailtrapEmailer struct {
	cfg    config.MailtrapConfig
	client *http.Client
}

func NewMailtrapEmailer(cfg config.MailtrapConfig) *MailtrapEmailer {
	return &MailtrapEmailer{
		cfg: cfg,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (e *MailtrapEmailer) Providers() []string {
	return []string{"mailtrap"}
}

func (e *MailtrapEmailer) SendIncident(incident storage.Incident, current storage.MonitorState) []SendResult {
	return []SendResult{e.SendProvider("mailtrap", incident, current)}
}

func (e *MailtrapEmailer) SendProvider(provider string, incident storage.Incident, current storage.MonitorState) SendResult {
	if provider != "mailtrap" {
		return SendResult{Provider: provider, Error: fmt.Errorf("mailtrap sender cannot send provider %q", provider)}
	}
	return SendResult{Provider: "mailtrap", Error: e.sendIncident(incident, current)}
}

func (e *MailtrapEmailer) sendIncident(incident storage.Incident, current storage.MonitorState) error {
	subject, body := buildIncidentContent(incident, current)
	payload := mailtrapMessage{
		From: mailtrapAddress{
			Email: e.cfg.From,
			Name:  e.cfg.FromName,
		},
		To:      mailtrapRecipients(e.cfg.To),
		Subject: subject,
		Text:    body,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, e.cfg.Endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+e.cfg.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("mailtrap send failed: status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

type mailtrapMessage struct {
	From    mailtrapAddress   `json:"from"`
	To      []mailtrapAddress `json:"to"`
	Subject string            `json:"subject"`
	Text    string            `json:"text"`
}

type mailtrapAddress struct {
	Email string `json:"email"`
	Name  string `json:"name,omitempty"`
}

func mailtrapRecipients(recipients []string) []mailtrapAddress {
	addresses := make([]mailtrapAddress, 0, len(recipients))
	for _, recipient := range recipients {
		addresses = append(addresses, mailtrapAddress{Email: recipient})
	}
	return addresses
}

func buildIncidentContent(incident storage.Incident, current storage.MonitorState) (string, string) {
	subject := fmt.Sprintf("[upag] %s %s", current.Name, incident.Transition)
	if current.MonitorID == observer.MonitorID {
		body := strings.Join([]string{
			fmt.Sprintf("Monitor: %s (%s)", current.Name, current.MonitorID),
			fmt.Sprintf("Transition: %s", incident.Transition),
			fmt.Sprintf("Observed at: %s", incident.ObservedAt.Format(time.RFC3339)),
			fmt.Sprintf("Error: %s", incident.Error),
			"",
		}, "\r\n")
		return subject, body
	}
	body := strings.Join([]string{
		fmt.Sprintf("Monitor: %s (%s)", current.Name, current.MonitorID),
		fmt.Sprintf("Transition: %s", incident.Transition),
		fmt.Sprintf("Observed at: %s", incident.ObservedAt.Format(time.RFC3339)),
		fmt.Sprintf("URL: %s", current.URL),
		fmt.Sprintf("Expected status: %d", current.ExpectedStatusCode),
		fmt.Sprintf("Observed status: %d", incident.StatusCode),
		fmt.Sprintf("Error: %s", incident.Error),
		"",
	}, "\r\n")
	return subject, body
}
