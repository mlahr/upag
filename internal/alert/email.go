package alert

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/smtp"
	"strings"
	"time"

	"upag/internal/config"
	"upag/internal/storage"
)

type Emailer struct {
	cfg config.SMTPConfig
}

func NewEmailer(cfg config.SMTPConfig) *Emailer {
	return &Emailer{cfg: cfg}
}

func (e *Emailer) SendIncident(incident storage.Incident, current storage.MonitorState) error {
	subject := fmt.Sprintf("[upag] %s %s", current.Name, incident.Transition)
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

	message := buildMessage(e.cfg.From, e.cfg.To, subject, body)
	addr := net.JoinHostPort(e.cfg.Host, fmt.Sprintf("%d", e.cfg.Port))
	auth := smtp.Auth(nil)
	if e.cfg.Username != "" || e.cfg.Password != "" {
		host := e.cfg.Host
		auth = smtp.PlainAuth("", e.cfg.Username, e.cfg.Password, host)
	}

	switch e.cfg.TLS {
	case "tls":
		return e.sendTLS(addr, auth, message)
	case "starttls":
		return e.sendStartTLS(addr, auth, message)
	case "none":
		return smtp.SendMail(addr, auth, e.cfg.From, e.cfg.To, message)
	default:
		return fmt.Errorf("unsupported smtp.tls mode %q", e.cfg.TLS)
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
