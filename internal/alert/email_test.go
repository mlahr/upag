package alert

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"upag/internal/config"
	"upag/internal/state"
	"upag/internal/storage"
)

func TestSendIncidentSMTPNone(t *testing.T) {
	addr, messages, stop := startFakeSMTP(t)
	defer stop()

	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	var port int
	if _, err := fmt.Sscanf(portText, "%d", &port); err != nil {
		t.Fatal(err)
	}

	emailer := NewEmailer(config.SMTPConfig{
		Host: host,
		Port: port,
		TLS:  "none",
		From: "alerts@example.com",
		To:   []string{"ops@example.com"},
	})
	err = emailer.SendIncident(storage.Incident{
		MonitorID:  "home",
		Name:       "Home",
		Transition: state.Down,
		ObservedAt: time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
		Error:      "timeout",
	}, storage.MonitorState{
		MonitorID:          "home",
		Name:               "Home",
		URL:                "https://example.com/",
		ExpectedStatusCode: 200,
	})
	if err != nil {
		t.Fatal(err)
	}

	message := <-messages
	if !strings.Contains(message, "Subject: [upag] Home DOWN") {
		t.Fatalf("message did not contain subject: %q", message)
	}
	if !strings.Contains(message, "Error: timeout") {
		t.Fatalf("message did not contain error: %q", message)
	}
}

func startFakeSMTP(t *testing.T) (string, <-chan string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	messages := make(chan string, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		writeLine := func(line string) {
			fmt.Fprintf(writer, "%s\r\n", line)
			writer.Flush()
		}
		writeLine("220 fake smtp")
		var data strings.Builder
		inData := false
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if inData {
				if line == "." {
					messages <- data.String()
					writeLine("250 ok")
					inData = false
					continue
				}
				data.WriteString(line)
				data.WriteString("\n")
				continue
			}
			upper := strings.ToUpper(line)
			switch {
			case strings.HasPrefix(upper, "EHLO"), strings.HasPrefix(upper, "HELO"):
				writeLine("250 fake")
			case strings.HasPrefix(upper, "MAIL FROM:"):
				writeLine("250 ok")
			case strings.HasPrefix(upper, "RCPT TO:"):
				writeLine("250 ok")
			case strings.HasPrefix(upper, "DATA"):
				writeLine("354 data")
				inData = true
			case strings.HasPrefix(upper, "QUIT"):
				writeLine("221 bye")
				return
			default:
				writeLine("250 ok")
			}
		}
	}()

	stop := func() {
		listener.Close()
		<-done
	}
	return listener.Addr().String(), messages, stop
}
