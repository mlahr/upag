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

func TestListUptimeStatsAggregatesProbeResultsByWindow(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	for _, probe := range []struct {
		monitorID string
		checkedAt time.Time
		ok        bool
	}{
		{monitorID: "home", checkedAt: now.Add(-24 * time.Hour), ok: true},
		{monitorID: "home", checkedAt: now.Add(-2 * time.Hour), ok: false},
		{monitorID: "home", checkedAt: now.Add(-1 * time.Hour), ok: true},
		{monitorID: "home", checkedAt: now.Add(-48 * time.Hour), ok: true},
		{monitorID: "home", checkedAt: now.Add(-8 * 24 * time.Hour), ok: false},
		{monitorID: "home", checkedAt: now.Add(-30 * 24 * time.Hour), ok: true},
		{monitorID: "home", checkedAt: now.Add(-31 * 24 * time.Hour), ok: false},
		{monitorID: "api", checkedAt: now.Add(-30 * time.Minute), ok: false},
	} {
		status := state.Down
		if probe.ok {
			status = state.Up
		}
		_, err := store.SaveProbeAndState(context.Background(), ProbeResult{
			MonitorID: probe.monitorID,
			CheckedAt: probe.checkedAt,
			OK:        probe.ok,
		}, MonitorState{
			MonitorID:     probe.monitorID,
			Name:          probe.monitorID,
			URL:           "https://example.com/",
			Status:        status,
			LastCheckedAt: probe.checkedAt,
			UpdatedAt:     probe.checkedAt,
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	stats, err := store.ListUptimeStats(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}

	home := stats["home"]
	if home.TwentyFourHour.TotalChecks != 3 || home.TwentyFourHour.SuccessfulChecks != 2 || home.TwentyFourHour.FailedChecks != 1 {
		t.Fatalf("home 24h stats = %+v, want 3 total, 2 successful, 1 failed", home.TwentyFourHour)
	}
	if !home.TwentyFourHour.WindowStartedAt.Equal(now.Add(-24*time.Hour)) || !home.TwentyFourHour.WindowEndedAt.Equal(now.Add(-time.Hour)) {
		t.Fatalf("home 24h window = %+v, want inclusive 24h boundary through latest probe", home.TwentyFourHour)
	}
	if home.SevenDay.TotalChecks != 4 || home.SevenDay.SuccessfulChecks != 3 || home.SevenDay.FailedChecks != 1 {
		t.Fatalf("home 7d stats = %+v, want 4 total, 3 successful, 1 failed", home.SevenDay)
	}
	if home.ThirtyDay.TotalChecks != 6 || home.ThirtyDay.SuccessfulChecks != 4 || home.ThirtyDay.FailedChecks != 2 {
		t.Fatalf("home 30d stats = %+v, want 6 total, 4 successful, 2 failed", home.ThirtyDay)
	}
	if !home.ThirtyDay.WindowStartedAt.Equal(now.Add(-30*24*time.Hour)) || !home.ThirtyDay.WindowEndedAt.Equal(now.Add(-time.Hour)) {
		t.Fatalf("home 30d window = %+v, want inclusive 30d boundary through latest probe", home.ThirtyDay)
	}
	if home.Retained.TotalChecks != 7 || home.Retained.SuccessfulChecks != 4 || home.Retained.FailedChecks != 3 {
		t.Fatalf("home retained stats = %+v, want 7 total, 4 successful, 3 failed", home.Retained)
	}

	api := stats["api"]
	if api.TwentyFourHour.TotalChecks != 1 || api.TwentyFourHour.SuccessfulChecks != 0 || api.TwentyFourHour.FailedChecks != 1 {
		t.Fatalf("api 24h stats = %+v, want one failed check", api.TwentyFourHour)
	}
}

func TestListUptimeStatsUsesZeroValueForEmptyWindows(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	checkedAt := now.Add(-8 * 24 * time.Hour)
	_, err = store.SaveProbeAndState(context.Background(), ProbeResult{
		MonitorID: "old",
		CheckedAt: checkedAt,
		OK:        false,
	}, MonitorState{
		MonitorID:     "old",
		Name:          "Old",
		URL:           "https://example.com/",
		Status:        state.Down,
		LastCheckedAt: checkedAt,
		UpdatedAt:     checkedAt,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	stats, err := store.ListUptimeStats(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}

	old := stats["old"]
	if old.TwentyFourHour.TotalChecks != 0 || !old.TwentyFourHour.WindowStartedAt.IsZero() || !old.TwentyFourHour.WindowEndedAt.IsZero() {
		t.Fatalf("old 24h stats = %+v, want zero-value empty window", old.TwentyFourHour)
	}
	if old.SevenDay.TotalChecks != 0 || !old.SevenDay.WindowStartedAt.IsZero() || !old.SevenDay.WindowEndedAt.IsZero() {
		t.Fatalf("old 7d stats = %+v, want zero-value empty window", old.SevenDay)
	}
	if old.ThirtyDay.TotalChecks != 1 || old.ThirtyDay.SuccessfulChecks != 0 || old.ThirtyDay.FailedChecks != 1 {
		t.Fatalf("old 30d stats = %+v, want one failed check", old.ThirtyDay)
	}
	if old.Retained.TotalChecks != 1 || old.Retained.SuccessfulChecks != 0 || old.Retained.FailedChecks != 1 {
		t.Fatalf("old retained stats = %+v, want one failed check", old.Retained)
	}
}

func TestMaintenanceWindowsRejectOverlapAndCanBeCancelled(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	saveTestState(t, store, "home", now)

	id, err := store.AddMaintenanceWindow(context.Background(), MaintenanceWindow{
		MonitorID: "home",
		StartsAt:  now.Add(time.Hour),
		EndsAt:    now.Add(2 * time.Hour),
		Reason:    "deploy",
		CreatedBy: "michael",
		CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("maintenance id = 0, want nonzero")
	}
	_, err = store.AddMaintenanceWindow(context.Background(), MaintenanceWindow{
		MonitorID: "home",
		StartsAt:  now.Add(90 * time.Minute),
		EndsAt:    now.Add(3 * time.Hour),
		Reason:    "overlap",
		CreatedBy: "michael",
		CreatedAt: now,
	})
	if err == nil {
		t.Fatal("overlapping maintenance window succeeded, want error")
	}
	if err := store.CancelMaintenanceWindow(context.Background(), id, now.Add(10*time.Minute), "michael", "done"); err != nil {
		t.Fatal(err)
	}
	nextID, err := store.AddMaintenanceWindow(context.Background(), MaintenanceWindow{
		MonitorID: "home",
		StartsAt:  now.Add(90 * time.Minute),
		EndsAt:    now.Add(3 * time.Hour),
		Reason:    "replacement",
		CreatedBy: "michael",
		CreatedAt: now,
	})
	if err != nil {
		t.Fatalf("add after cancellation: %v", err)
	}
	if nextID == id {
		t.Fatalf("replacement id = %d, want different id", nextID)
	}
}

func TestActiveMaintenanceWindowUsesInclusiveStartExclusiveEnd(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	saveTestState(t, store, "home", now)
	id, err := store.AddMaintenanceWindow(context.Background(), MaintenanceWindow{
		MonitorID: "home",
		StartsAt:  now,
		EndsAt:    now.Add(time.Hour),
		Reason:    "deploy",
		CreatedBy: "michael",
		CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	window, ok, err := store.ActiveMaintenanceWindow(context.Background(), "home", now)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || window.ID != id {
		t.Fatalf("active at start = %+v ok=%t, want id %d", window, ok, id)
	}
	_, ok, err = store.ActiveMaintenanceWindow(context.Background(), "home", now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("active at exclusive end = true, want false")
	}
}

func TestMaintenanceProbeResultsAreExcludedFromUptimeAndSyntheticIncidents(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	saveTestState(t, store, "home", now)
	maintenanceID, err := store.AddMaintenanceWindow(context.Background(), MaintenanceWindow{
		MonitorID: "home",
		StartsAt:  now.Add(-time.Hour),
		EndsAt:    now.Add(time.Hour),
		Reason:    "deploy",
		CreatedBy: "michael",
		CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, probe := range []struct {
		checkedAt           time.Time
		ok                  bool
		maintenanceWindowID int64
	}{
		{checkedAt: now.Add(-30 * time.Minute), ok: false, maintenanceWindowID: maintenanceID},
		{checkedAt: now.Add(-10 * time.Minute), ok: true},
	} {
		status := state.Down
		if probe.ok {
			status = state.Up
		}
		_, err := store.SaveProbeAndState(context.Background(), ProbeResult{
			MonitorID:           "home",
			CheckedAt:           probe.checkedAt,
			OK:                  probe.ok,
			Error:               "timeout",
			MaintenanceWindowID: probe.maintenanceWindowID,
		}, MonitorState{
			MonitorID:     "home",
			Name:          "Home",
			URL:           "https://example.com/",
			Status:        status,
			LastCheckedAt: probe.checkedAt,
			UpdatedAt:     probe.checkedAt,
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	stats, err := store.ListUptimeStats(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	retained := stats["home"].Retained
	if retained.TotalChecks != 2 || retained.SuccessfulChecks != 2 || retained.FailedChecks != 0 {
		t.Fatalf("retained stats = %+v, want two reportable successful checks", retained)
	}
	if retained.MaintenanceChecks != 1 || retained.MaintenanceFailedChecks != 1 {
		t.Fatalf("maintenance stats = %+v, want one failed maintenance check", retained)
	}
	incidents, err := store.ListIncidents(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 0 {
		t.Fatalf("incidents = %+v, want no synthetic incidents for maintenance-covered failure", incidents)
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

func TestListActionableAlertDeliveryFailures(t *testing.T) {
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
			NextRetryAt:   now.Add(time.Minute),
		},
		{
			IncidentID:    incidentID,
			MonitorID:     "home",
			Provider:      "smtp",
			AttemptedAt:   now.Add(time.Second),
			AttemptNumber: 2,
			Success:       false,
			Error:         "temporary failure again",
			NextRetryAt:   now.Add(2 * time.Minute),
		},
		{
			IncidentID:    incidentID,
			MonitorID:     "home",
			Provider:      "mailtrap",
			AttemptedAt:   now.Add(2 * time.Second),
			AttemptNumber: 1,
			Success:       false,
			Error:         "invalid token",
		},
		{
			IncidentID:    incidentID,
			MonitorID:     "home",
			Provider:      "mailtrap",
			AttemptedAt:   now.Add(3 * time.Second),
			AttemptNumber: 2,
			Success:       true,
		},
		{
			IncidentID:     incidentID,
			MonitorID:      "home",
			Provider:       "pager",
			AttemptedAt:    now.Add(4 * time.Second),
			AttemptNumber:  3,
			Success:        false,
			Error:          "permanent failure",
			RetryExhausted: true,
		},
	}); err != nil {
		t.Fatal(err)
	}

	failures, err := store.ListActionableAlertDeliveryFailures(context.Background(), 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != 2 {
		t.Fatalf("failure count = %d, want 2", len(failures))
	}
	byProvider := map[string]AlertNotification{}
	for _, failure := range failures {
		byProvider[failure.Provider] = failure
	}
	if byProvider["smtp"].AttemptNumber != 2 || byProvider["smtp"].Error != "temporary failure again" {
		t.Fatalf("smtp failure = %+v, want latest failed attempt", byProvider["smtp"])
	}
	if !byProvider["pager"].RetryExhausted {
		t.Fatalf("pager failure = %+v, want retry exhausted", byProvider["pager"])
	}
	if _, ok := byProvider["mailtrap"]; ok {
		t.Fatalf("mailtrap failure included despite later success: %+v", byProvider["mailtrap"])
	}
}

func TestListActionableAlertDeliveryFailuresAppliesLimit(t *testing.T) {
	store, incidentID, now := storeWithIncident(t)
	defer store.Close()

	for i := 0; i < 3; i++ {
		if err := store.SaveAlertNotifications(context.Background(), []AlertNotification{
			{
				IncidentID:    incidentID,
				MonitorID:     "home",
				Provider:      string(rune('a' + i)),
				AttemptedAt:   now.Add(time.Duration(i) * time.Second),
				AttemptNumber: 1,
				Success:       false,
				Error:         "failed",
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	failures, err := store.ListActionableAlertDeliveryFailures(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != 2 {
		t.Fatalf("failure count = %d, want 2", len(failures))
	}
}

func saveTestState(t *testing.T, store *Store, monitorID string, now time.Time) {
	t.Helper()
	_, err := store.SaveProbeAndState(context.Background(), ProbeResult{
		MonitorID: monitorID,
		CheckedAt: now,
		OK:        true,
	}, MonitorState{
		MonitorID:              monitorID,
		Name:                   monitorID,
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 state.Up,
		LastCheckedAt:          now,
		LastSuccessAt:          now,
		LastObservedStatusCode: 200,
		UpdatedAt:              now,
	}, nil)
	if err != nil {
		t.Fatal(err)
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
