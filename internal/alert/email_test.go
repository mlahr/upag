package alert

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"upag/internal/config"
	"upag/internal/state"
	"upag/internal/storage"
)

var testIncident = storage.Incident{
	MonitorID:  "home",
	Name:       "Home",
	Transition: state.Down,
	ObservedAt: time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
	Error:      "timeout",
}

var testMonitorState = storage.MonitorState{
	MonitorID:          "home",
	Name:               "Home",
	URL:                "https://example.com/",
	ExpectedStatusCode: 200,
}

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
	results := emailer.SendIncident(storage.Incident{
		MonitorID:  testIncident.MonitorID,
		Name:       testIncident.Name,
		Transition: testIncident.Transition,
		ObservedAt: testIncident.ObservedAt,
		Error:      testIncident.Error,
	}, storage.MonitorState{
		MonitorID:          testMonitorState.MonitorID,
		Name:               testMonitorState.Name,
		URL:                testMonitorState.URL,
		ExpectedStatusCode: testMonitorState.ExpectedStatusCode,
	})
	if err := SendResultsError(results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Provider != "smtp" {
		t.Fatalf("results = %+v, want one smtp result", results)
	}

	message := <-messages
	if !strings.Contains(message, "Subject: [upag] Home DOWN") {
		t.Fatalf("message did not contain subject: %q", message)
	}
	if !strings.Contains(message, "Error: timeout") {
		t.Fatalf("message did not contain error: %q", message)
	}
}

func TestSendIncidentMailtrap(t *testing.T) {
	var request struct {
		Authorization string
		ContentType   string
		Message       mailtrapMessage
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request.Authorization = r.Header.Get("Authorization")
		request.ContentType = r.Header.Get("Content-Type")
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/send" {
			t.Fatalf("path = %s, want /api/send", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&request.Message); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	emailer := NewMailtrapEmailer(config.MailtrapConfig{
		Token:    "token-123",
		Endpoint: server.URL + "/api/send",
		From:     "alerts@example.com",
		FromName: "upag",
		To:       []string{"ops@example.com"},
	})
	results := emailer.SendIncident(storage.Incident{
		MonitorID:  testIncident.MonitorID,
		Name:       testIncident.Name,
		Transition: testIncident.Transition,
		ObservedAt: testIncident.ObservedAt,
		Error:      testIncident.Error,
		StatusCode: testIncident.StatusCode,
	}, storage.MonitorState{
		MonitorID:          testMonitorState.MonitorID,
		Name:               testMonitorState.Name,
		URL:                testMonitorState.URL,
		ExpectedStatusCode: testMonitorState.ExpectedStatusCode,
	})
	if err := SendResultsError(results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Provider != "mailtrap" {
		t.Fatalf("results = %+v, want one mailtrap result", results)
	}
	if request.Authorization != "Bearer token-123" {
		t.Fatalf("authorization = %q, want bearer token", request.Authorization)
	}
	if request.ContentType != "application/json" {
		t.Fatalf("content type = %q, want application/json", request.ContentType)
	}
	if request.Message.From.Email != "alerts@example.com" {
		t.Fatalf("from email = %q, want alerts@example.com", request.Message.From.Email)
	}
	if request.Message.From.Name != "upag" {
		t.Fatalf("from name = %q, want upag", request.Message.From.Name)
	}
	if len(request.Message.To) != 1 || request.Message.To[0].Email != "ops@example.com" {
		t.Fatalf("to = %#v, want ops@example.com", request.Message.To)
	}
	if request.Message.Subject != "[upag] Home DOWN" {
		t.Fatalf("subject = %q, want [upag] Home DOWN", request.Message.Subject)
	}
	if !strings.Contains(request.Message.Text, "Error: timeout") {
		t.Fatalf("text did not contain error: %q", request.Message.Text)
	}
}

func TestSendIncidentMailtrapRejectsNon2xx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "invalid token", http.StatusUnauthorized)
	}))
	defer server.Close()

	emailer := NewMailtrapEmailer(config.MailtrapConfig{
		Token:    "bad-token",
		Endpoint: server.URL,
		From:     "alerts@example.com",
		To:       []string{"ops@example.com"},
	})
	results := emailer.SendIncident(storage.Incident{
		MonitorID:  testIncident.MonitorID,
		Name:       testIncident.Name,
		Transition: testIncident.Transition,
		ObservedAt: testIncident.ObservedAt,
	}, storage.MonitorState{
		MonitorID:          testMonitorState.MonitorID,
		Name:               testMonitorState.Name,
		URL:                testMonitorState.URL,
		ExpectedStatusCode: testMonitorState.ExpectedStatusCode,
	})
	err := SendResultsError(results)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "status=401") {
		t.Fatalf("error = %q, want status=401", err)
	}
}

func TestMultiSenderSendsToAllProviders(t *testing.T) {
	first := &fakeSender{}
	second := &fakeSender{}
	sender := MultiSender{senders: []IncidentSender{first, second}}

	results := sender.SendIncident(testIncident, testMonitorState)
	if err := SendResultsError(results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("result count = %d, want 2", len(results))
	}
	if first.calls != 1 {
		t.Fatalf("first calls = %d, want 1", first.calls)
	}
	if second.calls != 1 {
		t.Fatalf("second calls = %d, want 1", second.calls)
	}
}

func TestMultiSenderAttemptsAllProvidersAndJoinsErrors(t *testing.T) {
	firstErr := errors.New("first failed")
	secondErr := errors.New("second failed")
	first := &fakeSender{err: firstErr}
	second := &fakeSender{err: secondErr}
	sender := MultiSender{senders: []IncidentSender{first, second}}

	results := sender.SendIncident(testIncident, testMonitorState)
	err := SendResultsError(results)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, firstErr) {
		t.Fatalf("error %q does not contain first error", err)
	}
	if !errors.Is(err, secondErr) {
		t.Fatalf("error %q does not contain second error", err)
	}
	if first.calls != 1 {
		t.Fatalf("first calls = %d, want 1", first.calls)
	}
	if second.calls != 1 {
		t.Fatalf("second calls = %d, want 1", second.calls)
	}
}

type fakeSender struct {
	calls int
	err   error
}

func (s *fakeSender) SendIncident(storage.Incident, storage.MonitorState) []SendResult {
	s.calls++
	return []SendResult{{Provider: "fake", Error: s.err}}
}

func (s *fakeSender) Providers() []string {
	return []string{"fake"}
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
