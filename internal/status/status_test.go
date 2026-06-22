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
	states      []storage.MonitorState
	uptime      map[string]storage.UptimeStats
	failures    []storage.AlertNotification
	maintenance []storage.MaintenanceWindow
	observer    storage.ObserverState
	observerOK  bool
	sentinels   []storage.ObserverSentinelResult
}

func (s fakeStore) ListStates(context.Context) ([]storage.MonitorState, error) {
	return s.states, nil
}

func (s fakeStore) ListUptimeStats(context.Context, time.Time, int) (map[string]storage.UptimeStats, error) {
	return s.uptime, nil
}

func (s fakeStore) ListActionableAlertDeliveryFailures(context.Context, int) ([]storage.AlertNotification, error) {
	return s.failures, nil
}

func (s fakeStore) ListMaintenanceWindows(context.Context, storage.MaintenanceWindowFilter) ([]storage.MaintenanceWindow, error) {
	return s.maintenance, nil
}

func (s fakeStore) GetObserverState(context.Context) (storage.ObserverState, bool, error) {
	return s.observer, s.observerOK, nil
}

func (s fakeStore) ListObserverSentinelResults(context.Context) ([]storage.ObserverSentinelResult, error) {
	return s.sentinels, nil
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
	now := time.Now().UTC()
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
		uptime: map[string]storage.UptimeStats{
			"home": {
				TwentyFourHour: storage.UptimeWindowStats{
					TotalChecks:             3,
					SuccessfulChecks:        2,
					FailedChecks:            1,
					MaintenanceChecks:       2,
					MaintenanceFailedChecks: 1,
					DowntimeSeconds:         60,
					ReportableSeconds:       600,
					WindowStartedAt:         checkedAt.Add(-10 * time.Minute),
					WindowEndedAt:           checkedAt,
				},
				SevenDay: storage.UptimeWindowStats{
					TotalChecks:       4,
					SuccessfulChecks:  3,
					FailedChecks:      1,
					DowntimeSeconds:   60,
					ReportableSeconds: 240,
					WindowStartedAt:   checkedAt.Add(-24 * time.Hour),
					WindowEndedAt:     checkedAt,
				},
				ThirtyDay: storage.UptimeWindowStats{
					TotalChecks:       6,
					SuccessfulChecks:  5,
					FailedChecks:      1,
					DowntimeSeconds:   60,
					ReportableSeconds: 360,
					WindowStartedAt:   checkedAt.Add(-29 * 24 * time.Hour),
					WindowEndedAt:     checkedAt,
				},
				Retained: storage.UptimeWindowStats{
					TotalChecks:       6,
					SuccessfulChecks:  5,
					FailedChecks:      1,
					DowntimeSeconds:   60,
					ReportableSeconds: 360,
					WindowStartedAt:   checkedAt.Add(-48 * time.Hour),
					WindowEndedAt:     checkedAt,
				},
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
		maintenance: []storage.MaintenanceWindow{
			{
				ID:        7,
				MonitorID: "home",
				StartsAt:  now.Add(-time.Minute),
				EndsAt:    now.Add(time.Hour),
				Reason:    "deploy",
				CreatedBy: "michael",
				CreatedAt: now.Add(-2 * time.Minute),
			},
			{
				ID:        8,
				MonitorID: "home",
				StartsAt:  now.Add(2 * time.Hour),
				EndsAt:    now.Add(3 * time.Hour),
				Reason:    "database migration",
				CreatedBy: "michael",
				CreatedAt: now,
			},
		},
		observer: storage.ObserverState{
			Status:               "OBSERVER_UP",
			ConsecutiveSuccesses: 2,
			LastCheckedAt:        checkedAt,
			LastSuccessAt:        checkedAt,
			UpdatedAt:            checkedAt,
		},
		observerOK: true,
		sentinels: []storage.ObserverSentinelResult{
			{
				SentinelID:         "gstatic",
				Name:               "Google connectivity check",
				URL:                "https://www.gstatic.com/generate_204",
				ExpectedStatusCode: 204,
				OK:                 true,
				ObservedStatusCode: 204,
				LatencyMS:          12,
				CheckedAt:          checkedAt,
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
		Observer     struct {
			Status               string `json:"status"`
			ConsecutiveSuccesses int    `json:"consecutive_successes"`
			LastCheckedAt        string `json:"last_checked_at"`
			Sentinels            []struct {
				ID                 string `json:"id"`
				OK                 bool   `json:"ok"`
				ExpectedStatusCode int    `json:"expected_status_code"`
				ObservedStatusCode int    `json:"observed_status_code"`
				LatencyMS          int64  `json:"latency_ms"`
			} `json:"sentinels"`
		} `json:"observer"`
		Monitors []struct {
			ID            string  `json:"id"`
			LastCheckedAt string  `json:"last_checked_at"`
			LastFailureAt *string `json:"last_failure_at"`
			Uptime        struct {
				TwentyFourHour struct {
					TotalChecks             int      `json:"total_checks"`
					SuccessfulChecks        int      `json:"successful_checks"`
					FailedChecks            int      `json:"failed_checks"`
					MaintenanceChecks       int      `json:"maintenance_checks"`
					MaintenanceFailedChecks int      `json:"maintenance_failed_checks"`
					DowntimeSeconds         int64    `json:"downtime_seconds"`
					ReportableSeconds       int64    `json:"reportable_seconds"`
					UptimePercent           *float64 `json:"uptime_percent"`
					WindowStartedAt         string   `json:"window_started_at"`
					WindowEndedAt           string   `json:"window_ended_at"`
				} `json:"24h"`
				SevenDay struct {
					TotalChecks   int      `json:"total_checks"`
					UptimePercent *float64 `json:"uptime_percent"`
				} `json:"7d"`
				ThirtyDay struct {
					TotalChecks   int      `json:"total_checks"`
					UptimePercent *float64 `json:"uptime_percent"`
				} `json:"30d"`
				Retained struct {
					TotalChecks   int      `json:"total_checks"`
					UptimePercent *float64 `json:"uptime_percent"`
				} `json:"retained"`
			} `json:"uptime"`
			ActiveMaintenance *struct {
				ID     int64  `json:"id"`
				Reason string `json:"reason"`
			} `json:"active_maintenance"`
			UpcomingMaintenance []struct {
				ID     int64  `json:"id"`
				Reason string `json:"reason"`
			} `json:"upcoming_maintenance"`
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
	if body.Observer.Status != "OBSERVER_UP" || body.Observer.ConsecutiveSuccesses != 2 || body.Observer.LastCheckedAt != "2026-06-22T02:20:04Z" {
		t.Fatalf("observer = %+v, want observer up with last check", body.Observer)
	}
	if len(body.Observer.Sentinels) != 1 || body.Observer.Sentinels[0].ID != "gstatic" || !body.Observer.Sentinels[0].OK || body.Observer.Sentinels[0].ExpectedStatusCode != 204 || body.Observer.Sentinels[0].ObservedStatusCode != 204 || body.Observer.Sentinels[0].LatencyMS != 12 {
		t.Fatalf("observer sentinels = %+v, want gstatic success", body.Observer.Sentinels)
	}
	if len(body.Monitors) != 1 || body.Monitors[0].ID != "home" || body.Monitors[0].LastCheckedAt != "2026-06-22T02:20:04Z" {
		t.Fatalf("monitors = %+v, want home with last_checked_at", body.Monitors)
	}
	if body.Monitors[0].LastFailureAt != nil {
		t.Fatalf("last_failure_at = %#v, want null", body.Monitors[0].LastFailureAt)
	}
	uptime24h := body.Monitors[0].Uptime.TwentyFourHour
	if uptime24h.TotalChecks != 3 || uptime24h.SuccessfulChecks != 2 || uptime24h.FailedChecks != 1 {
		t.Fatalf("24h uptime counts = %+v, want 3 total, 2 successful, 1 failed", uptime24h)
	}
	if uptime24h.MaintenanceChecks != 2 || uptime24h.MaintenanceFailedChecks != 1 {
		t.Fatalf("24h maintenance counts = %+v, want 2 maintenance checks and 1 failed maintenance check", uptime24h)
	}
	if uptime24h.DowntimeSeconds != 60 || uptime24h.ReportableSeconds != 600 {
		t.Fatalf("24h duration stats = %+v, want 60 downtime seconds and 600 reportable seconds", uptime24h)
	}
	if uptime24h.UptimePercent == nil || *uptime24h.UptimePercent != 90 {
		t.Fatalf("24h uptime_percent = %#v, want 90", uptime24h.UptimePercent)
	}
	if uptime24h.WindowStartedAt != "2026-06-22T02:10:04Z" || uptime24h.WindowEndedAt != "2026-06-22T02:20:04Z" {
		t.Fatalf("24h uptime window = %+v, want observed start and end", uptime24h)
	}
	if body.Monitors[0].Uptime.SevenDay.TotalChecks != 4 || body.Monitors[0].Uptime.SevenDay.UptimePercent == nil || *body.Monitors[0].Uptime.SevenDay.UptimePercent != 75 {
		t.Fatalf("7d uptime = %+v, want 4 checks at 75 percent by duration", body.Monitors[0].Uptime.SevenDay)
	}
	if body.Monitors[0].Uptime.ThirtyDay.TotalChecks != 6 || body.Monitors[0].Uptime.ThirtyDay.UptimePercent == nil || *body.Monitors[0].Uptime.ThirtyDay.UptimePercent != 83.33 {
		t.Fatalf("30d uptime = %+v, want 6 checks at 83.33 percent by duration", body.Monitors[0].Uptime.ThirtyDay)
	}
	if body.Monitors[0].Uptime.Retained.TotalChecks != 6 || body.Monitors[0].Uptime.Retained.UptimePercent == nil || *body.Monitors[0].Uptime.Retained.UptimePercent != 83.33 {
		t.Fatalf("retained uptime = %+v, want 6 checks at 83.33 percent by duration", body.Monitors[0].Uptime.Retained)
	}
	if body.Monitors[0].ActiveMaintenance == nil || body.Monitors[0].ActiveMaintenance.ID != 7 || body.Monitors[0].ActiveMaintenance.Reason != "deploy" {
		t.Fatalf("active maintenance = %+v, want deploy window", body.Monitors[0].ActiveMaintenance)
	}
	if len(body.Monitors[0].UpcomingMaintenance) != 1 || body.Monitors[0].UpcomingMaintenance[0].ID != 8 {
		t.Fatalf("upcoming maintenance = %+v, want window 8", body.Monitors[0].UpcomingMaintenance)
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

func TestListenAddressFormatsHostAndPort(t *testing.T) {
	tests := map[string]struct {
		address string
		port    int
		want    string
	}{
		"ipv4": {
			address: "127.0.0.1",
			port:    8080,
			want:    "127.0.0.1:8080",
		},
		"ipv6": {
			address: "::1",
			port:    8080,
			want:    "[::1]:8080",
		},
		"hostname": {
			address: "localhost",
			port:    8080,
			want:    "localhost:8080",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if got := ListenAddress(test.address, test.port); got != test.want {
				t.Fatalf("ListenAddress(%q, %d) = %q, want %q", test.address, test.port, got, test.want)
			}
		})
	}
}
