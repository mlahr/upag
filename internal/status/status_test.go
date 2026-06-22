package status

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"upag/internal/storage"
)

type fakeStore struct {
	states   []storage.MonitorState
	failures []storage.AlertNotification
}

func (s fakeStore) ListStates(context.Context) ([]storage.MonitorState, error) {
	return s.states, nil
}

func (s fakeStore) ListActionableAlertDeliveryFailures(context.Context, int) ([]storage.AlertNotification, error) {
	return s.failures, nil
}

func TestHealthReturnsLivenessJSON(t *testing.T) {
	startedAt := time.Date(2026, 6, 22, 2, 15, 4, 0, time.UTC)
	handler := NewHandler(fakeStore{}, func() Metadata {
		return Metadata{Version: "test", StartedAt: startedAt}
	})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200", recorder.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" || body["version"] != "test" {
		t.Fatalf("health body = %+v, want status ok and version test", body)
	}
	if body["started_at"] != "2026-06-22T02:15:04Z" {
		t.Fatalf("started_at = %#v, want RFC3339 UTC timestamp", body["started_at"])
	}
}

func TestStatusReturnsMonitorStateAndAlertFailures(t *testing.T) {
	startedAt := time.Date(2026, 6, 22, 2, 15, 4, 0, time.UTC)
	checkedAt := time.Date(2026, 6, 22, 2, 20, 4, 0, time.UTC)
	handler := NewHandler(fakeStore{
		states: []storage.MonitorState{
			{
				MonitorID:              "home",
				Name:                   "Homepage",
				URL:                    "https://example.com/",
				Status:                 "UP",
				LastCheckedAt:          checkedAt,
				LastSuccessAt:          checkedAt,
				LastObservedStatusCode: 200,
				UpdatedAt:              checkedAt,
			},
		},
		failures: []storage.AlertNotification{
			{
				IncidentID:    42,
				MonitorID:     "api",
				Provider:      "smtp",
				AttemptedAt:   checkedAt,
				AttemptNumber: 2,
				Error:         "send failed",
				NextRetryAt:   checkedAt.Add(5 * time.Minute),
			},
		},
	}, func() Metadata {
		return Metadata{
			Version:      "test",
			StartedAt:    startedAt,
			ConfigPath:   "/etc/upag/config.yaml",
			MonitorCount: 1,
		}
	})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/status", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200", recorder.Code)
	}
	var body struct {
		Status       string `json:"status"`
		Version      string `json:"version"`
		ConfigPath   string `json:"config_path"`
		MonitorCount int    `json:"monitor_count"`
		Monitors     []struct {
			ID            string  `json:"id"`
			LastCheckedAt string  `json:"last_checked_at"`
			LastFailureAt *string `json:"last_failure_at"`
		} `json:"monitors"`
		AlertDeliveryFailures []struct {
			IncidentID    int64  `json:"incident_id"`
			Provider      string `json:"provider"`
			AttemptNumber int    `json:"attempt_number"`
			Error         string `json:"error"`
		} `json:"alert_delivery_failures"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "ok" || body.Version != "test" || body.ConfigPath != "/etc/upag/config.yaml" || body.MonitorCount != 1 {
		t.Fatalf("status metadata = %+v, want configured metadata", body)
	}
	if len(body.Monitors) != 1 || body.Monitors[0].ID != "home" || body.Monitors[0].LastCheckedAt != "2026-06-22T02:20:04Z" {
		t.Fatalf("monitors = %+v, want home with last_checked_at", body.Monitors)
	}
	if body.Monitors[0].LastFailureAt != nil {
		t.Fatalf("last_failure_at = %#v, want null", body.Monitors[0].LastFailureAt)
	}
	if len(body.AlertDeliveryFailures) != 1 || body.AlertDeliveryFailures[0].IncidentID != 42 || body.AlertDeliveryFailures[0].Provider != "smtp" || body.AlertDeliveryFailures[0].AttemptNumber != 2 || body.AlertDeliveryFailures[0].Error != "send failed" {
		t.Fatalf("alert_delivery_failures = %+v, want smtp failure", body.AlertDeliveryFailures)
	}
}

func TestHandlerRejectsUnsupportedMethod(t *testing.T) {
	handler := NewHandler(fakeStore{}, func() Metadata { return Metadata{} })
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/status", strings.NewReader("")))

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status code = %d, want 405", recorder.Code)
	}
	if recorder.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("Allow = %q, want GET", recorder.Header().Get("Allow"))
	}
}

func TestHandlerReturnsNotFound(t *testing.T) {
	handler := NewHandler(fakeStore{}, func() Metadata { return Metadata{} })
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/missing", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status code = %d, want 404", recorder.Code)
	}
}
