package checker

import (
	"context"
	"net/http"
	"net/http/httptest"
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
