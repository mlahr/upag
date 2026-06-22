package checker

import (
	"context"
	"net/http"
	"net/http/httptest"
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
