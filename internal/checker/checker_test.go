package checker

import (
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"upag/internal/config"
)

func TestCheckRequiresExactStatusCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
	})
	if result.OK {
		t.Fatal("expected check to fail for non-exact status code")
	}
	if result.ObservedStatusCode != http.StatusNoContent {
		t.Fatalf("observed status = %d, want 204", result.ObservedStatusCode)
	}
}

func TestCheckDoesNotFollowRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/target", http.StatusFound)
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusFound,
		Timeout:            config.Duration{Duration: time.Second},
	})
	if !result.OK {
		t.Fatalf("expected redirect response to satisfy exact 302 check, got error %q", result.Error)
	}
}

func TestCheckSucceedsWhenResponseBodyContainsRequiredString(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Welcome to the homepage"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustContain: "Welcome",
		},
	})
	if !result.OK {
		t.Fatalf("expected response body assertion to pass, got error %q", result.Error)
	}
}

func TestCheckFailsWhenResponseBodyDoesNotContainRequiredString(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Temporarily unavailable"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustContain: "Welcome",
		},
	})
	if result.OK {
		t.Fatal("expected response body assertion to fail")
	}
	if !strings.Contains(result.Error, `does not contain required string "Welcome"`) {
		t.Fatalf("error = %q, want required string failure", result.Error)
	}
}

func TestCheckSucceedsWhenResponseBodyDoesNotContainForbiddenString(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Welcome to the homepage"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustNotContain: "Maintenance mode",
		},
	})
	if !result.OK {
		t.Fatalf("expected forbidden response body assertion to pass, got error %q", result.Error)
	}
}

func TestCheckFailsWhenResponseBodyContainsForbiddenString(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Maintenance mode"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustNotContain: "Maintenance mode",
		},
	})
	if result.OK {
		t.Fatal("expected forbidden response body assertion to fail")
	}
	if !strings.Contains(result.Error, `contains forbidden string "Maintenance mode"`) {
		t.Fatalf("error = %q, want forbidden string failure", result.Error)
	}
}

func TestCheckStatusMismatchSkipsResponseBodyAssertions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("Welcome"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustContain: "Welcome",
		},
	})
	if result.OK {
		t.Fatal("expected status mismatch to fail")
	}
	if result.Error != "expected HTTP status 200, observed HTTP status 503" {
		t.Fatalf("error = %q, want status mismatch error", result.Error)
	}
}

func TestCheckSucceedsWhenResponseBodyCommandExitsZero(t *testing.T) {
	t.Setenv("UPAG_CHECKER_HELPER_PROCESS", "1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Welcome to the homepage"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			Command:        helperCommand("contains", "Welcome"),
			CommandTimeout: config.Duration{Duration: time.Second},
		},
	})
	if !result.OK {
		t.Fatalf("expected response body command assertion to pass, got error %q", result.Error)
	}
}

func TestCheckPassesExactResponseBodyBytesToCommandStdin(t *testing.T) {
	t.Setenv("UPAG_CHECKER_HELPER_PROCESS", "1")
	body := []byte{0xff, 'o', 'k', 0x00}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			Command:        helperCommand("hex", hex.EncodeToString(body)),
			CommandTimeout: config.Duration{Duration: time.Second},
		},
	})
	if !result.OK {
		t.Fatalf("expected exact stdin bytes to pass, got error %q", result.Error)
	}
}

func TestCheckFailsWhenResponseBodyCommandExitsNonZero(t *testing.T) {
	t.Setenv("UPAG_CHECKER_HELPER_PROCESS", "1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("bad response"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			Command:        helperCommand("fail"),
			CommandTimeout: config.Duration{Duration: time.Second},
		},
	})
	if result.OK {
		t.Fatal("expected response body command assertion to fail")
	}
	if !strings.Contains(result.Error, `response body command`) || !strings.Contains(result.Error, "exit status 7") || !strings.Contains(result.Error, `stderr="helper failure"`) {
		t.Fatalf("error = %q, want command failure with stderr", result.Error)
	}
}

func TestCheckFailsWhenResponseBodyCommandTimesOut(t *testing.T) {
	t.Setenv("UPAG_CHECKER_HELPER_PROCESS", "1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("slow command"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			Command:        helperCommand("sleep", "200ms"),
			CommandTimeout: config.Duration{Duration: 20 * time.Millisecond},
		},
	})
	if result.OK {
		t.Fatal("expected response body command assertion to time out")
	}
	if !strings.Contains(result.Error, "exceeded timeout 20ms") {
		t.Fatalf("error = %q, want command timeout failure", result.Error)
	}
}

func TestCheckStatusMismatchSkipsResponseBodyCommand(t *testing.T) {
	t.Setenv("UPAG_CHECKER_HELPER_PROCESS", "1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("bad response"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			Command:        helperCommand("fail"),
			CommandTimeout: config.Duration{Duration: time.Second},
		},
	})
	if result.OK {
		t.Fatal("expected status mismatch to fail")
	}
	if result.Error != "expected HTTP status 200, observed HTTP status 503" {
		t.Fatalf("error = %q, want status mismatch error", result.Error)
	}
}

func TestCheckWithoutResponseBodyAssertionsKeepsStatusOnlyBehavior(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("any response body"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
	})
	if !result.OK {
		t.Fatalf("expected status-only check to pass, got error %q", result.Error)
	}
}

func TestCheckSucceedsWhenResponseTimeIsAtOrBelowMaximum(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		MaxResponseTime:    config.Duration{Duration: time.Second},
	})
	if !result.OK {
		t.Fatalf("expected response time assertion to pass, got error %q", result.Error)
	}
	if result.ResponseTime <= 0 {
		t.Fatalf("response time = %s, want positive duration", result.ResponseTime)
	}
}

func helperCommand(args ...string) []string {
	command := []string{os.Args[0], "-test.run=TestResponseBodyCommandHelperProcess", "--"}
	return append(command, args...)
}

func TestResponseBodyCommandHelperProcess(t *testing.T) {
	if os.Getenv("UPAG_CHECKER_HELPER_PROCESS") != "1" {
		return
	}
	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) < 2 {
		os.Exit(2)
	}
	body, err := io.ReadAll(os.Stdin)
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	switch args[1] {
	case "contains":
		if len(args) != 3 || !strings.Contains(string(body), args[2]) {
			os.Exit(1)
		}
	case "hex":
		if len(args) != 3 || hex.EncodeToString(body) != args[2] {
			os.Exit(1)
		}
	case "fail":
		_, _ = os.Stderr.WriteString("helper failure")
		os.Exit(7)
	case "sleep":
		if len(args) != 3 {
			os.Exit(2)
		}
		duration, err := time.ParseDuration(args[2])
		if err != nil {
			os.Exit(2)
		}
		time.Sleep(duration)
	default:
		os.Exit(2)
	}
	os.Exit(0)
}

func TestCheckFailsWhenResponseTimeExceedsMaximum(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte("slow body"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		MaxResponseTime:    config.Duration{Duration: time.Millisecond},
	})
	if result.OK {
		t.Fatal("expected response time assertion to fail")
	}
	if !strings.Contains(result.Error, "response time") || !strings.Contains(result.Error, "exceeded maximum 1ms") {
		t.Fatalf("error = %q, want response time failure", result.Error)
	}
}

func TestCheckRecordsLatencyAndResponseTimeSeparately(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(30 * time.Millisecond)
		_, _ = w.Write([]byte("delayed body"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		MaxResponseTime:    config.Duration{Duration: time.Second},
	})
	if !result.OK {
		t.Fatalf("expected check to pass, got error %q", result.Error)
	}
	if result.Latency <= 0 {
		t.Fatalf("latency = %s, want positive duration", result.Latency)
	}
	if result.ResponseTime <= result.Latency {
		t.Fatalf("response time = %s, want greater than latency %s", result.ResponseTime, result.Latency)
	}
}

func TestCheckStatusMismatchPrecedesResponseTimeFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(20 * time.Millisecond)
		_, _ = w.Write([]byte("slow failure"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		MaxResponseTime:    config.Duration{Duration: time.Millisecond},
	})
	if result.OK {
		t.Fatal("expected status mismatch to fail")
	}
	if result.Error != "expected HTTP status 200, observed HTTP status 503" {
		t.Fatalf("error = %q, want status mismatch error", result.Error)
	}
	if result.ResponseTime <= 0 {
		t.Fatalf("response time = %s, want positive duration", result.ResponseTime)
	}
}
