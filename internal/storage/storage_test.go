package storage

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"upag/internal/state"
)

func TestTenantContextDefaultsToDefaultTenant(t *testing.T) {
	ctx := WithTenant(context.Background(), "")
	if got := TenantFromContext(ctx); got != defaultTenantID {
		t.Fatalf("tenant = %q, want %q", got, defaultTenantID)
	}
}

func TestTenantContextTrimsWhitespaceToDefaultTenant(t *testing.T) {
	ctx := WithTenant(context.Background(), "   ")
	if got := TenantFromContext(ctx); got != defaultTenantID {
		t.Fatalf("tenant = %q, want %q", got, defaultTenantID)
	}
}

func TestTenantContextFallsBackToDefaultWithoutTenantValue(t *testing.T) {
	if got := TenantFromContext(context.Background()); got != defaultTenantID {
		t.Fatalf("tenant = %q, want %q", got, defaultTenantID)
	}
}

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
	if !columns["attempt_count"] {
		t.Fatal("probe_results.attempt_count column is missing")
	}
}

func TestOpenRecordsSchemaMigrationsAndDoesNotReapplyThem(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	firstCount := migrationCount(t, store)
	if firstCount != len(migrations()) {
		t.Fatalf("schema migration count = %d, want %d", firstCount, len(migrations()))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	secondCount := migrationCount(t, store)
	if secondCount != firstCount {
		t.Fatalf("schema migration count after reopen = %d, want %d", secondCount, firstCount)
	}
}

func TestFailedMigrationIsNotRecorded(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	err = store.applyMigration(context.Background(), migration{
		ID: "9999_failing_migration",
		Fn: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `ALTER TABLE table_that_does_not_exist ADD COLUMN value TEXT`)
			return err
		},
	})
	if err == nil {
		t.Fatal("expected migration failure")
	}

	var count int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM schema_migrations WHERE id = ?`, "9999_failing_migration").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed migration records = %d, want 0", count)
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
	if !columns["attempt_count"] {
		t.Fatal("probe_results.attempt_count column is missing after migration")
	}
	if migrationCount(t, store) != len(migrations()) {
		t.Fatalf("schema migration count = %d, want %d", migrationCount(t, store), len(migrations()))
	}
}

func migrationCount(t *testing.T, store *Store) int {
	t.Helper()
	var count int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func tableCount(t *testing.T, store *Store, table string) int {
	t.Helper()
	var count int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
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

	stats, err := store.ListUptimeStats(context.Background(), now, 3)
	if err != nil {
		t.Fatal(err)
	}

	home := stats["home"]
	if home.TwentyFourHour.TotalChecks != 3 || home.TwentyFourHour.SuccessfulChecks != 2 || home.TwentyFourHour.FailedChecks != 1 {
		t.Fatalf("home 24h stats = %+v, want 3 total, 2 successful, 1 failed", home.TwentyFourHour)
	}
	if home.TwentyFourHour.DowntimeSeconds != 0 {
		t.Fatalf("home 24h downtime_seconds = %d, want 0 for isolated failure below threshold", home.TwentyFourHour.DowntimeSeconds)
	}
	if home.TwentyFourHour.ReportableSeconds != int64((24*time.Hour)/time.Second) {
		t.Fatalf("home 24h reportable_seconds = %d, want 86400", home.TwentyFourHour.ReportableSeconds)
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

	stats, err := store.ListUptimeStats(context.Background(), now, 3)
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

func TestListUptimeStatsUsesStrictAccountingForConfirmedOutages(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	for _, probe := range []struct {
		checkedAt time.Time
		ok        bool
	}{
		{checkedAt: now.Add(-10 * time.Minute), ok: true},
		{checkedAt: now.Add(-5 * time.Minute), ok: false},
		{checkedAt: now.Add(-4 * time.Minute), ok: false},
		{checkedAt: now.Add(-3 * time.Minute), ok: false},
		{checkedAt: now.Add(-1 * time.Minute), ok: true},
	} {
		status := state.Down
		if probe.ok {
			status = state.Up
		}
		_, err := store.SaveProbeAndState(context.Background(), ProbeResult{
			MonitorID: "home",
			CheckedAt: probe.checkedAt,
			OK:        probe.ok,
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

	stats, err := store.ListUptimeStats(context.Background(), now, 3)
	if err != nil {
		t.Fatal(err)
	}
	retained := stats["home"].Retained
	if retained.DowntimeSeconds != int64((4*time.Minute)/time.Second) {
		t.Fatalf("retained downtime_seconds = %d, want 240", retained.DowntimeSeconds)
	}
	if retained.ReportableSeconds != int64((10*time.Minute)/time.Second) {
		t.Fatalf("retained reportable_seconds = %d, want 600", retained.ReportableSeconds)
	}
}

func TestSaveProbeAndStateMaintainsStatusIntervals(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	base := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	for _, step := range []struct {
		at     time.Time
		status string
		ok     bool
	}{
		{at: base, status: state.Up, ok: true},
		{at: base.Add(time.Minute), status: state.Up, ok: true},
		{at: base.Add(2 * time.Minute), status: state.Failing, ok: false},
		{at: base.Add(3 * time.Minute), status: state.Up, ok: true},
		{at: base.Add(4 * time.Minute), status: state.Failing, ok: false},
		{at: base.Add(5 * time.Minute), status: state.Down, ok: false},
		{at: base.Add(6 * time.Minute), status: state.Up, ok: true},
	} {
		_, err := store.SaveProbeAndState(context.Background(), ProbeResult{
			MonitorID: "home",
			CheckedAt: step.at,
			OK:        step.ok,
		}, MonitorState{
			MonitorID:     "home",
			Name:          "Home",
			URL:           "https://example.com/",
			Status:        step.status,
			LastCheckedAt: step.at,
			UpdatedAt:     step.at,
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	intervals := readStatusIntervals(t, store, "home")
	if len(intervals) != 6 {
		t.Fatalf("interval count = %d, want 6: %+v", len(intervals), intervals)
	}
	if intervals[0].status != state.Up || !intervals[0].startedAt.Equal(base) || !intervals[0].endedAt.Equal(base.Add(2*time.Minute)) {
		t.Fatalf("first interval = %+v, want coalesced UP from base to +2m", intervals[0])
	}
	if intervals[1].status != state.Failing || intervals[1].downtime {
		t.Fatalf("recovered failing interval = %+v, want non-downtime FAILING", intervals[1])
	}
	if intervals[3].status != state.Failing || !intervals[3].downtime {
		t.Fatalf("confirmed failing interval = %+v, want downtime FAILING", intervals[3])
	}
	if intervals[4].status != state.Down || !intervals[4].downtime {
		t.Fatalf("down interval = %+v, want downtime DOWN", intervals[4])
	}
	if intervals[5].status != state.Up || !intervals[5].endedAt.IsZero() {
		t.Fatalf("open interval = %+v, want open UP", intervals[5])
	}
}

func TestEnsureStatusIntervalsBackfilledIsIdempotent(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	for i, ok := range []bool{true, false, false, false, true} {
		err := store.SaveProbeResult(context.Background(), ProbeResult{
			MonitorID: "home",
			CheckedAt: now.Add(time.Duration(i) * time.Minute),
			OK:        ok,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := store.EnsureStatusIntervalsBackfilled(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	firstCount := tableCount(t, store, "monitor_status_intervals")
	if err := store.EnsureStatusIntervalsBackfilled(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	secondCount := tableCount(t, store, "monitor_status_intervals")
	if firstCount != secondCount {
		t.Fatalf("interval count after second backfill = %d, want %d", secondCount, firstCount)
	}
	intervals := readStatusIntervals(t, store, "home")
	if len(intervals) != 3 {
		t.Fatalf("intervals = %+v, want up/down/up", intervals)
	}
	if intervals[1].status != state.Down || !intervals[1].downtime {
		t.Fatalf("backfilled outage = %+v, want downtime DOWN", intervals[1])
	}
}

func TestListStatusIntervalsFiltersAndLimits(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	base := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	for _, item := range []struct {
		monitorID string
		status    string
		at        time.Time
	}{
		{monitorID: "home", status: state.Up, at: base},
		{monitorID: "api", status: state.Down, at: base.Add(time.Minute)},
		{monitorID: "home", status: state.Down, at: base.Add(2 * time.Minute)},
	} {
		_, err := store.SaveProbeAndState(context.Background(), ProbeResult{
			MonitorID: item.monitorID,
			CheckedAt: item.at,
			OK:        item.status == state.Up,
		}, MonitorState{
			MonitorID:     item.monitorID,
			Name:          item.monitorID,
			URL:           "https://example.com/",
			Status:        item.status,
			LastCheckedAt: item.at,
			UpdatedAt:     item.at,
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	intervals, err := store.ListStatusIntervals(context.Background(), StatusIntervalFilter{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(intervals) != 2 {
		t.Fatalf("interval count = %d, want 2", len(intervals))
	}
	if intervals[0].MonitorID != "home" || intervals[0].Status != state.Down {
		t.Fatalf("latest interval = %+v, want home DOWN", intervals[0])
	}
	filtered, err := store.ListStatusIntervals(context.Background(), StatusIntervalFilter{MonitorID: "api", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 1 || filtered[0].MonitorID != "api" {
		t.Fatalf("filtered intervals = %+v, want one api interval", filtered)
	}
}

func TestRollupAndPruneProbeResultsCompactsRawProbes(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	saveTestState(t, store, "home", now)
	maintenanceID, err := store.AddMaintenanceWindow(context.Background(), MaintenanceWindow{
		MonitorID: "home",
		StartsAt:  now.Add(-48 * time.Hour),
		EndsAt:    now.Add(-47 * time.Hour),
		Reason:    "deploy",
		CreatedBy: "michael",
		CreatedAt: now.Add(-49 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, probe := range []ProbeResult{
		{MonitorID: "home", CheckedAt: now.Add(-25 * time.Hour), OK: false},
		{MonitorID: "home", CheckedAt: now.Add(-25*time.Hour + time.Minute), OK: false},
		{MonitorID: "home", CheckedAt: now.Add(-25*time.Hour + 2*time.Minute), OK: false},
		{MonitorID: "home", CheckedAt: now.Add(-25*time.Hour + 4*time.Minute), OK: true},
		{MonitorID: "home", CheckedAt: now.Add(-47*time.Hour - 30*time.Minute), OK: false, MaintenanceWindowID: maintenanceID},
		{MonitorID: "home", CheckedAt: now.Add(-23 * time.Hour), OK: true},
	} {
		_, err := store.SaveProbeAndState(context.Background(), probe, MonitorState{
			MonitorID:     probe.MonitorID,
			Name:          "Home",
			URL:           "https://example.com/",
			Status:        state.Up,
			LastCheckedAt: probe.CheckedAt,
			UpdatedAt:     probe.CheckedAt,
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = store.RollupAndPruneProbeResults(context.Background(), ProbeRetentionPolicy{
		ProbeResults:       RollupRetention{Duration: 24 * time.Hour},
		ProbeMinuteRollups: RollupRetention{Duration: 30 * 24 * time.Hour},
		ProbeHourlyRollups: RollupRetention{Duration: 365 * 24 * time.Hour},
		ProbeDailyRollups:  RollupRetention{Forever: true},
	}, now)
	if err != nil {
		t.Fatal(err)
	}

	if got := tableCount(t, store, "probe_results"); got != 2 {
		t.Fatalf("probe_results count = %d, want two recent raw probes", got)
	}
	if got := tableCount(t, store, "probe_minute_rollups"); got != 5 {
		t.Fatalf("probe_minute_rollups count = %d, want five compacted minutes", got)
	}
	if got := tableCount(t, store, "probe_outcome_runs"); got != 2 {
		t.Fatalf("probe_outcome_runs count = %d, want failure and success runs", got)
	}

	stats, err := store.ListUptimeStats(context.Background(), now, 3)
	if err != nil {
		t.Fatal(err)
	}
	retained := stats["home"].Retained
	if retained.TotalChecks != 6 || retained.SuccessfulChecks != 3 || retained.FailedChecks != 3 {
		t.Fatalf("retained stats = %+v, want six reportable checks with three successes", retained)
	}
	if retained.MaintenanceChecks != 1 || retained.MaintenanceFailedChecks != 1 {
		t.Fatalf("maintenance stats = %+v, want one failed maintenance check", retained)
	}
	if retained.DowntimeSeconds != int64((4*time.Minute)/time.Second) {
		t.Fatalf("retained downtime_seconds = %d, want 240 from compacted outage run", retained.DowntimeSeconds)
	}
}

func TestRollupAndPruneProbeResultsCompactsRollupLevels(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	old := now.Add(-40 * 24 * time.Hour)
	for _, probe := range []ProbeResult{
		{MonitorID: "home", CheckedAt: old, OK: true},
		{MonitorID: "home", CheckedAt: old.Add(10 * time.Minute), OK: false},
	} {
		_, err = store.SaveProbeAndState(context.Background(), probe, MonitorState{
			MonitorID:     "home",
			Name:          "Home",
			URL:           "https://example.com/",
			Status:        state.Up,
			LastCheckedAt: probe.CheckedAt,
			UpdatedAt:     probe.CheckedAt,
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = store.RollupAndPruneProbeResults(context.Background(), ProbeRetentionPolicy{
		ProbeResults:       RollupRetention{Duration: 24 * time.Hour},
		ProbeMinuteRollups: RollupRetention{Duration: 30 * 24 * time.Hour},
		ProbeHourlyRollups: RollupRetention{Duration: 365 * 24 * time.Hour},
		ProbeDailyRollups:  RollupRetention{Forever: true},
	}, now)
	if err != nil {
		t.Fatal(err)
	}

	if got := tableCount(t, store, "probe_minute_rollups"); got != 0 {
		t.Fatalf("probe_minute_rollups count = %d, want rolled into hourly", got)
	}
	if got := tableCount(t, store, "probe_hourly_rollups"); got != 1 {
		t.Fatalf("probe_hourly_rollups count = %d, want one hourly rollup", got)
	}
	stats, err := store.ListUptimeStats(context.Background(), now, 3)
	if err != nil {
		t.Fatal(err)
	}
	retained := stats["home"].Retained
	if retained.TotalChecks != 2 || retained.SuccessfulChecks != 1 || retained.FailedChecks != 1 {
		t.Fatalf("retained stats = %+v, want two checks rolled into one hour", retained)
	}
	if got := tableCount(t, store, "probe_daily_rollups"); got != 0 {
		t.Fatalf("probe_daily_rollups count = %d, want no daily rollup before hourly retention", got)
	}
}

func TestRollupAndPruneProbeResultsMergesIncrementalBucketCompaction(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	base := time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC)
	for _, probe := range []ProbeResult{
		{MonitorID: "home", CheckedAt: base.Add(10 * time.Second), OK: true},
		{MonitorID: "home", CheckedAt: base.Add(50 * time.Second), OK: false},
	} {
		_, err := store.SaveProbeAndState(context.Background(), probe, MonitorState{
			MonitorID:     "home",
			Name:          "Home",
			URL:           "https://example.com/",
			Status:        state.Up,
			LastCheckedAt: probe.CheckedAt,
			UpdatedAt:     probe.CheckedAt,
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	policy := ProbeRetentionPolicy{
		ProbeResults:       RollupRetention{Duration: time.Hour},
		ProbeMinuteRollups: RollupRetention{Duration: 30 * 24 * time.Hour},
		ProbeHourlyRollups: RollupRetention{Duration: 365 * 24 * time.Hour},
		ProbeDailyRollups:  RollupRetention{Forever: true},
	}
	if err := store.RollupAndPruneProbeResults(context.Background(), policy, base.Add(time.Hour+30*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.RollupAndPruneProbeResults(context.Background(), policy, base.Add(time.Hour+time.Minute)); err != nil {
		t.Fatal(err)
	}

	if got := tableCount(t, store, "probe_results"); got != 0 {
		t.Fatalf("probe_results count = %d, want all raw probes compacted", got)
	}
	if got := tableCount(t, store, "probe_minute_rollups"); got != 1 {
		t.Fatalf("probe_minute_rollups count = %d, want one merged minute bucket", got)
	}
	stats, err := store.ListUptimeStats(context.Background(), base.Add(2*time.Hour), 3)
	if err != nil {
		t.Fatal(err)
	}
	retained := stats["home"].Retained
	if retained.TotalChecks != 2 || retained.SuccessfulChecks != 1 || retained.FailedChecks != 1 {
		t.Fatalf("retained stats = %+v, want merged minute bucket with two checks", retained)
	}
	if !retained.WindowStartedAt.Equal(base.Add(10*time.Second)) || !retained.WindowEndedAt.Equal(base.Add(50*time.Second)) {
		t.Fatalf("retained window = %+v, want first and last compacted probe timestamps", retained)
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

	stats, err := store.ListUptimeStats(context.Background(), now, 3)
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

func TestListDueAlertNotificationRetriesIncludesObserverIncident(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	incident := &Incident{
		MonitorID:  "__observer__",
		Name:       "Observer Connectivity",
		Transition: state.ObserverDown,
		ObservedAt: now,
		Error:      "observer timeout",
	}
	incidentID, err := store.SaveObserverCheck(context.Background(), ObserverState{
		Status:              state.ObserverDown,
		ConsecutiveFailures: 3,
		LastCheckedAt:       now,
		LastFailureAt:       now,
		LastError:           "observer timeout",
		UpdatedAt:           now,
	}, nil, incident)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveAlertNotifications(context.Background(), []AlertNotification{
		{
			IncidentID:    incidentID,
			MonitorID:     "__observer__",
			Provider:      "smtp",
			AttemptedAt:   now,
			AttemptNumber: 1,
			Success:       false,
			Error:         "temporary failure",
			NextRetryAt:   now.Add(-time.Minute),
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
	if retries[0].CurrentState.MonitorID != "__observer__" || retries[0].CurrentState.Status != state.ObserverDown || retries[0].CurrentState.LastError != "observer timeout" {
		t.Fatalf("current state = %+v, want observer state", retries[0].CurrentState)
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

type testStatusInterval struct {
	status    string
	startedAt time.Time
	endedAt   time.Time
	downtime  bool
}

func readStatusIntervals(t *testing.T, store *Store, monitorID string) []testStatusInterval {
	t.Helper()
	rows, err := store.db.QueryContext(context.Background(), `SELECT status, started_at, ended_at, downtime
		FROM monitor_status_intervals
		WHERE monitor_id = ?
		ORDER BY started_at, id`, monitorID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var intervals []testStatusInterval
	for rows.Next() {
		var interval testStatusInterval
		var startedAt string
		var endedAt sql.NullString
		var downtime int
		if err := rows.Scan(&interval.status, &startedAt, &endedAt, &downtime); err != nil {
			t.Fatal(err)
		}
		interval.startedAt = parseTime(startedAt)
		interval.endedAt = parseNullTime(endedAt)
		interval.downtime = intBool(downtime)
		intervals = append(intervals, interval)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return intervals
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
