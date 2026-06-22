package storage

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"upag/internal/state"
)

func TestSaveProbeAndStatePersistsIncident(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	next := MonitorState{
		MonitorID:              "home",
		Name:                   "Home",
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 state.Down,
		ConsecutiveFailures:    3,
		LastCheckedAt:          now,
		LastFailureAt:          now,
		LastError:              "timeout",
		LastObservedStatusCode: 0,
		UpdatedAt:              now,
	}
	incident := &Incident{
		MonitorID:  "home",
		Name:       "Home",
		Transition: state.Down,
		ObservedAt: now,
		Error:      "timeout",
	}
	incidentID, err := store.SaveProbeAndState(context.Background(), ProbeResult{
		MonitorID: "home",
		CheckedAt: now,
		OK:        false,
		Error:     "timeout",
	}, next, incident)
	if err != nil {
		t.Fatal(err)
	}
	if incidentID == 0 {
		t.Fatal("incident id = 0, want nonzero")
	}

	loaded, ok, err := store.GetState(context.Background(), "home")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("state not found")
	}
	if loaded.Status != state.Down || loaded.ConsecutiveFailures != 3 {
		t.Fatalf("loaded state = %+v", loaded)
	}
	incidents, err := store.ListIncidents(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 1 {
		t.Fatalf("incident count = %d, want 1", len(incidents))
	}
}

func TestListIncidentsIncludesFailedProbeBeforeTransition(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	next := MonitorState{
		MonitorID:              "api",
		Name:                   "API",
		URL:                    "https://example.com/x",
		ExpectedStatusCode:     200,
		Status:                 state.Failing,
		ConsecutiveFailures:    1,
		LastCheckedAt:          now,
		LastFailureAt:          now,
		LastError:              "expected HTTP status 200, observed HTTP status 404",
		LastObservedStatusCode: 404,
		UpdatedAt:              now,
	}
	incidentID, err := store.SaveProbeAndState(context.Background(), ProbeResult{
		MonitorID:          "api",
		CheckedAt:          now,
		OK:                 false,
		ObservedStatusCode: 404,
		Error:              "expected HTTP status 200, observed HTTP status 404",
	}, next, nil)
	if err != nil {
		t.Fatal(err)
	}
	if incidentID != 0 {
		t.Fatalf("incident id = %d, want 0", incidentID)
	}

	incidents, err := store.ListIncidents(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 1 {
		t.Fatalf("incident count = %d, want 1", len(incidents))
	}
	got := incidents[0]
	if got.Transition != "FAILURE" {
		t.Fatalf("event = %q, want FAILURE", got.Transition)
	}
	if got.Name != "API" || got.StatusCode != 404 {
		t.Fatalf("incident = %+v, want API failure with status 404", got)
	}
}

func TestDeleteStatesExceptRemovesStaleMonitorStates(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	for _, id := range []string{"active", "stale"} {
		next := MonitorState{
			MonitorID:              id,
			Name:                   id,
			URL:                    "https://example.com/",
			ExpectedStatusCode:     200,
			Status:                 state.Up,
			LastCheckedAt:          now,
			LastSuccessAt:          now,
			LastObservedStatusCode: 200,
			UpdatedAt:              now,
		}
		_, err := store.SaveProbeAndState(context.Background(), ProbeResult{
			MonitorID:          id,
			CheckedAt:          now,
			OK:                 true,
			ObservedStatusCode: 200,
		}, next, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := store.DeleteStatesExcept(context.Background(), []string{"active"}); err != nil {
		t.Fatal(err)
	}
	states, err := store.ListStates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 {
		t.Fatalf("state count = %d, want 1", len(states))
	}
	if states[0].MonitorID != "active" {
		t.Fatalf("remaining monitor = %q, want active", states[0].MonitorID)
	}
}

func TestOpenCreatesProbeResultsResponseTimeColumn(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	columns, err := store.tableColumns(context.Background(), "probe_results")
	if err != nil {
		t.Fatal(err)
	}
	if !columns["response_time_ms"] {
		t.Fatal("probe_results.response_time_ms column is missing")
	}
}

func TestOpenMigratesProbeResultsResponseTimeColumn(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE probe_results (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		monitor_id TEXT NOT NULL,
		checked_at TEXT NOT NULL,
		ok INTEGER NOT NULL,
		observed_status_code INTEGER NOT NULL,
		latency_ms INTEGER NOT NULL,
		error TEXT NOT NULL
	)`)
	if closeErr := db.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}

	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	columns, err := store.tableColumns(context.Background(), "probe_results")
	if err != nil {
		t.Fatal(err)
	}
	if !columns["response_time_ms"] {
		t.Fatal("probe_results.response_time_ms column is missing after migration")
	}
}

func TestSaveProbeAndStatePersistsResponseTime(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	next := MonitorState{
		MonitorID:              "home",
		Name:                   "Home",
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 state.Up,
		LastCheckedAt:          now,
		LastSuccessAt:          now,
		LastObservedStatusCode: 200,
		UpdatedAt:              now,
	}
	_, err = store.SaveProbeAndState(context.Background(), ProbeResult{
		MonitorID:          "home",
		CheckedAt:          now,
		OK:                 true,
		ObservedStatusCode: 200,
		LatencyMS:          12,
		ResponseTimeMS:     34,
	}, next, nil)
	if err != nil {
		t.Fatal(err)
	}

	var got int64
	if err := store.db.QueryRowContext(context.Background(), `SELECT response_time_ms FROM probe_results WHERE monitor_id = ?`, "home").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 34 {
		t.Fatalf("response_time_ms = %d, want 34", got)
	}
}

func TestSaveAlertNotificationsPersistsAttempts(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	next := MonitorState{
		MonitorID:              "home",
		Name:                   "Home",
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 state.Down,
		ConsecutiveFailures:    3,
		LastCheckedAt:          now,
		LastFailureAt:          now,
		LastError:              "timeout",
		LastObservedStatusCode: 0,
		UpdatedAt:              now,
	}
	incidentID, err := store.SaveProbeAndState(context.Background(), ProbeResult{
		MonitorID: "home",
		CheckedAt: now,
		OK:        false,
		Error:     "timeout",
	}, next, &Incident{
		MonitorID:  "home",
		Name:       "Home",
		Transition: state.Down,
		ObservedAt: now,
		Error:      "timeout",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.SaveAlertNotifications(context.Background(), []AlertNotification{
		{
			IncidentID:    incidentID,
			MonitorID:     "home",
			Provider:      "smtp",
			AttemptedAt:   now,
			AttemptNumber: 1,
			Success:       true,
		},
		{
			IncidentID:    incidentID,
			MonitorID:     "home",
			Provider:      "mailtrap",
			AttemptedAt:   now,
			AttemptNumber: 1,
			Success:       false,
			Error:         "invalid token",
			NextRetryAt:   now.Add(time.Minute),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	notifications, err := store.ListAlertNotifications(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(notifications) != 2 {
		t.Fatalf("notification count = %d, want 2", len(notifications))
	}
	byProvider := map[string]AlertNotification{}
	for _, notification := range notifications {
		byProvider[notification.Provider] = notification
	}
	if byProvider["smtp"].IncidentID != incidentID || !byProvider["smtp"].Success {
		t.Fatalf("smtp notification = %+v, want success linked to incident %d", byProvider["smtp"], incidentID)
	}
	if byProvider["mailtrap"].IncidentID != incidentID || byProvider["mailtrap"].Success || byProvider["mailtrap"].Error != "invalid token" {
		t.Fatalf("mailtrap notification = %+v, want failure linked to incident %d", byProvider["mailtrap"], incidentID)
	}
	if byProvider["mailtrap"].AttemptNumber != 1 || !byProvider["mailtrap"].NextRetryAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("mailtrap retry metadata = %+v, want attempt 1 with next retry", byProvider["mailtrap"])
	}
}

func TestListDueAlertNotificationRetries(t *testing.T) {
	store, incidentID, now := storeWithIncident(t)
	defer store.Close()

	if err := store.SaveAlertNotifications(context.Background(), []AlertNotification{
		{
			IncidentID:    incidentID,
			MonitorID:     "home",
			Provider:      "smtp",
			AttemptedAt:   now,
			AttemptNumber: 1,
			Success:       false,
			Error:         "temporary failure",
			NextRetryAt:   now.Add(-time.Minute),
		},
		{
			IncidentID:    incidentID,
			MonitorID:     "home",
			Provider:      "mailtrap",
			AttemptedAt:   now,
			AttemptNumber: 1,
			Success:       false,
			Error:         "rate limited",
			NextRetryAt:   now.Add(time.Minute),
		},
		{
			IncidentID:     incidentID,
			MonitorID:      "home",
			Provider:       "pager",
			AttemptedAt:    now,
			AttemptNumber:  3,
			Success:        false,
			Error:          "permanent failure",
			RetryExhausted: true,
		},
	}); err != nil {
		t.Fatal(err)
	}

	retries, err := store.ListDueAlertNotificationRetries(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(retries) != 1 {
		t.Fatalf("retry count = %d, want 1", len(retries))
	}
	if retries[0].Notification.Provider != "smtp" {
		t.Fatalf("provider = %q, want smtp", retries[0].Notification.Provider)
	}
	if retries[0].Incident.ID != incidentID {
		t.Fatalf("incident id = %d, want %d", retries[0].Incident.ID, incidentID)
	}
	if retries[0].CurrentState.MonitorID != "home" || retries[0].CurrentState.URL == "" {
		t.Fatalf("current state = %+v, want home state", retries[0].CurrentState)
	}
}

func TestListDueAlertNotificationRetriesExcludesLaterSuccess(t *testing.T) {
	store, incidentID, now := storeWithIncident(t)
	defer store.Close()

	if err := store.SaveAlertNotifications(context.Background(), []AlertNotification{
		{
			IncidentID:    incidentID,
			MonitorID:     "home",
			Provider:      "smtp",
			AttemptedAt:   now,
			AttemptNumber: 1,
			Success:       false,
			Error:         "temporary failure",
			NextRetryAt:   now.Add(-time.Minute),
		},
		{
			IncidentID:    incidentID,
			MonitorID:     "home",
			Provider:      "smtp",
			AttemptedAt:   now.Add(time.Second),
			AttemptNumber: 2,
			Success:       true,
		},
	}); err != nil {
		t.Fatal(err)
	}

	retries, err := store.ListDueAlertNotificationRetries(context.Background(), now.Add(2*time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(retries) != 0 {
		t.Fatalf("retry count = %d, want 0", len(retries))
	}
}

func storeWithIncident(t *testing.T) (*Store, int64, time.Time) {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	next := MonitorState{
		MonitorID:              "home",
		Name:                   "Home",
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 state.Down,
		ConsecutiveFailures:    3,
		LastCheckedAt:          now,
		LastFailureAt:          now,
		LastError:              "timeout",
		LastObservedStatusCode: 0,
		UpdatedAt:              now,
	}
	incidentID, err := store.SaveProbeAndState(context.Background(), ProbeResult{
		MonitorID: "home",
		CheckedAt: now,
		OK:        false,
		Error:     "timeout",
	}, next, &Incident{
		MonitorID:  "home",
		Name:       "Home",
		Transition: state.Down,
		ObservedAt: now,
		Error:      "timeout",
	})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	return store, incidentID, now
}
