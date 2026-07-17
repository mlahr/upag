package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"upag/internal/state"
)

type conformanceBackend struct {
	name string
	open func(*testing.T) Backend
}

func conformanceBackends() []conformanceBackend {
	return []conformanceBackend{
		{
			name: "sqlite",
			open: func(t *testing.T) Backend {
				t.Helper()
				store, err := Open(filepath.Join(t.TempDir(), "test.sqlite"))
				if err != nil {
					t.Fatal(err)
				}
				return store
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) Backend {
				t.Helper()
				store, err := OpenPostgres(context.Background(), postgresTestDSN)
				if err != nil {
					t.Fatal(err)
				}
				resetPostgresTestData(t, store)
				return store
			},
		},
	}
}

func TestStorageConformanceProbeStateIncidentAndSyntheticFailures(t *testing.T) {
	for _, backend := range conformanceBackends() {
		t.Run(backend.name, func(t *testing.T) {
			store := backend.open(t)
			defer store.Close()

			ctx := context.Background()
			now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
			incidentID, err := store.SaveProbeAndState(ctx, ProbeResult{
				MonitorID:          "home",
				CheckedAt:          now,
				OK:                 false,
				ObservedStatusCode: 503,
				LatencyMS:          12,
				ResponseTimeMS:     34,
				AttemptCount:       3,
				Error:              "timeout",
			}, MonitorState{
				MonitorID:              "home",
				Name:                   "Home",
				URL:                    "https://example.com/",
				ExpectedStatusCode:     200,
				Status:                 state.Down,
				ConsecutiveFailures:    3,
				LastCheckedAt:          now,
				LastFailureAt:          now,
				LastError:              "timeout",
				LastObservedStatusCode: 503,
				UpdatedAt:              now,
			}, &Incident{
				MonitorID:  "home",
				Name:       "Home",
				Transition: state.Down,
				ObservedAt: now,
				Error:      "timeout",
				StatusCode: 503,
			})
			if err != nil {
				t.Fatal(err)
			}
			if incidentID == 0 {
				t.Fatal("incident id = 0, want nonzero")
			}

			loaded, ok, err := store.GetState(ctx, "home")
			if err != nil {
				t.Fatal(err)
			}
			if !ok || loaded.Status != state.Down || loaded.ConsecutiveFailures != 3 || loaded.LastObservedStatusCode != 503 {
				t.Fatalf("loaded state = %+v ok=%t, want DOWN state", loaded, ok)
			}

			failAt := now.Add(time.Minute)
			if _, err := store.SaveProbeAndState(ctx, ProbeResult{
				MonitorID:          "api",
				CheckedAt:          failAt,
				OK:                 false,
				ObservedStatusCode: 404,
				Error:              "expected HTTP status 200, observed HTTP status 404",
			}, MonitorState{
				MonitorID:              "api",
				Name:                   "API",
				URL:                    "https://example.com/api",
				ExpectedStatusCode:     200,
				Status:                 state.Failing,
				ConsecutiveFailures:    1,
				LastCheckedAt:          failAt,
				LastFailureAt:          failAt,
				LastError:              "expected HTTP status 200, observed HTTP status 404",
				LastObservedStatusCode: 404,
				UpdatedAt:              failAt,
			}, nil); err != nil {
				t.Fatal(err)
			}

			incidents, err := store.ListIncidents(ctx, IncidentFilter{Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(incidents) != 2 {
				t.Fatalf("incident count = %d, want declared incident plus synthetic failure", len(incidents))
			}
			if incidents[0].Transition != "FAILURE" || incidents[0].Name != "API" || incidents[0].StatusCode != 404 {
				t.Fatalf("latest incident = %+v, want API synthetic failure", incidents[0])
			}
			if incidents[1].Transition != state.Down || incidents[1].Name != "Home" || incidents[1].StatusCode != 503 {
				t.Fatalf("older incident = %+v, want Home DOWN incident", incidents[1])
			}
			recentIncidents, err := store.ListIncidents(ctx, IncidentFilter{Limit: 10, Since: failAt})
			if err != nil {
				t.Fatal(err)
			}
			if len(recentIncidents) != 1 || recentIncidents[0].MonitorID != "api" {
				t.Fatalf("recent incidents = %+v, want only API synthetic failure", recentIncidents)
			}
		})
	}
}

func TestStorageConformanceStatusIntervals(t *testing.T) {
	for _, backend := range conformanceBackends() {
		t.Run(backend.name, func(t *testing.T) {
			store := backend.open(t)
			defer store.Close()

			ctx := context.Background()
			base := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
			for _, step := range []struct {
				status string
				at     time.Time
				ok     bool
			}{
				{status: state.Up, at: base, ok: true},
				{status: state.Failing, at: base.Add(time.Minute), ok: false},
				{status: state.Down, at: base.Add(2 * time.Minute), ok: false},
				{status: state.Up, at: base.Add(3 * time.Minute), ok: true},
			} {
				_, err := store.SaveProbeAndState(ctx, ProbeResult{
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

			intervals, err := store.ListStatusIntervals(ctx, StatusIntervalFilter{MonitorID: "home", Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(intervals) != 4 {
				t.Fatalf("interval count = %d, want 4: %+v", len(intervals), intervals)
			}
			if intervals[0].Status != state.Up || !intervals[0].EndedAt.IsZero() {
				t.Fatalf("latest interval = %+v, want open UP", intervals[0])
			}
			if intervals[1].Status != state.Down || !intervals[1].Downtime {
				t.Fatalf("down interval = %+v, want downtime DOWN", intervals[1])
			}
			if intervals[2].Status != state.Failing || !intervals[2].Downtime {
				t.Fatalf("failing interval = %+v, want retroactive downtime FAILING", intervals[2])
			}
			recentIntervals, err := store.ListStatusIntervals(ctx, StatusIntervalFilter{
				MonitorID: "home",
				Limit:     10,
				Since:     base.Add(90 * time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(recentIntervals) != 3 {
				t.Fatalf("recent interval count = %d, want 3 overlapping intervals: %+v", len(recentIntervals), recentIntervals)
			}
			if recentIntervals[2].Status != state.Failing {
				t.Fatalf("oldest recent interval = %+v, want overlapping FAILING interval", recentIntervals[2])
			}
		})
	}
}

func TestStorageConformanceMaintenanceAwareStatusIntervals(t *testing.T) {
	for _, backend := range conformanceBackends() {
		t.Run(backend.name, func(t *testing.T) {
			store := backend.open(t)
			defer store.Close()

			ctx := context.Background()
			base := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
			saveIntervalState := func(monitorID string, status string, at time.Time, maintenanceWindowID int64) {
				t.Helper()
				_, err := store.SaveProbeAndState(ctx, ProbeResult{
					MonitorID:           monitorID,
					CheckedAt:           at,
					OK:                  status == state.Up,
					MaintenanceWindowID: maintenanceWindowID,
				}, MonitorState{
					MonitorID:               monitorID,
					Name:                    monitorID,
					URL:                     "https://example.com/",
					ExpectedStatusCode:      200,
					Status:                  status,
					StatusBeforeMaintenance: state.Down,
					LastCheckedAt:           at,
					UpdatedAt:               at,
				}, nil)
				if err != nil {
					t.Fatal(err)
				}
			}

			saveIntervalState("home", state.Down, base, 0)
			windowID, err := store.AddMaintenanceWindow(ctx, MaintenanceWindow{
				MonitorID: "home",
				StartsAt:  base.Add(time.Hour),
				EndsAt:    base.Add(2 * time.Hour),
				Reason:    "deploy",
				CreatedBy: "tester",
				CreatedAt: base,
			})
			if err != nil {
				t.Fatal(err)
			}
			saveIntervalState("home", state.Maintenance, base.Add(65*time.Minute), windowID)
			saveIntervalState("home", state.Down, base.Add(125*time.Minute), 0)

			intervals, err := store.ListStatusIntervals(ctx, StatusIntervalFilter{
				MonitorID: "home",
				Limit:     10,
				Now:       base.Add(3 * time.Hour),
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(intervals) != 3 {
				t.Fatalf("intervals = %+v, want DOWN/MAINTENANCE/DOWN", intervals)
			}
			if intervals[0].Status != state.Down || !intervals[0].Downtime || !intervals[0].StartedAt.Equal(base.Add(2*time.Hour)) || !intervals[0].EndedAt.IsZero() {
				t.Fatalf("latest interval = %+v, want open DOWN from maintenance end", intervals[0])
			}
			if intervals[1].Status != state.Maintenance || intervals[1].Downtime || !intervals[1].StartedAt.Equal(base.Add(time.Hour)) || !intervals[1].EndedAt.Equal(base.Add(2*time.Hour)) {
				t.Fatalf("maintenance interval = %+v, want exact non-downtime window", intervals[1])
			}
			if intervals[2].Status != state.Down || !intervals[2].Downtime || !intervals[2].StartedAt.Equal(base) || !intervals[2].EndedAt.Equal(base.Add(time.Hour)) {
				t.Fatalf("oldest interval = %+v, want DOWN ending at maintenance start", intervals[2])
			}

			limited, err := store.ListStatusIntervals(ctx, StatusIntervalFilter{MonitorID: "home", Limit: 2, Now: base.Add(3 * time.Hour)})
			if err != nil {
				t.Fatal(err)
			}
			if len(limited) != 2 || limited[0].Status != state.Down || limited[1].Status != state.Maintenance {
				t.Fatalf("limited intervals = %+v, want limit applied after projection", limited)
			}
			recent, err := store.ListStatusIntervals(ctx, StatusIntervalFilter{
				MonitorID: "home",
				Limit:     10,
				Since:     base.Add(90 * time.Minute),
				Now:       base.Add(3 * time.Hour),
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(recent) != 2 || recent[0].Status != state.Down || recent[1].Status != state.Maintenance {
				t.Fatalf("recent intervals = %+v, want intervals overlapping --since", recent)
			}

			saveIntervalState("cancelled", state.Up, base, 0)
			cancelledID, err := store.AddMaintenanceWindow(ctx, MaintenanceWindow{
				MonitorID: "cancelled",
				StartsAt:  base.Add(time.Hour),
				EndsAt:    base.Add(2 * time.Hour),
				Reason:    "cancelled deploy",
				CreatedBy: "tester",
				CreatedAt: base,
			})
			if err != nil {
				t.Fatal(err)
			}
			saveIntervalState("cancelled", state.Maintenance, base.Add(65*time.Minute), cancelledID)
			if err := store.CancelMaintenanceWindow(ctx, cancelledID, base.Add(70*time.Minute), "tester", "aborted"); err != nil {
				t.Fatal(err)
			}
			cancelled, err := store.ListStatusIntervals(ctx, StatusIntervalFilter{MonitorID: "cancelled", Limit: 10, Now: base.Add(3 * time.Hour)})
			if err != nil {
				t.Fatal(err)
			}
			if len(cancelled) != 1 || cancelled[0].Status != state.Up || !cancelled[0].EndedAt.IsZero() {
				t.Fatalf("cancelled-window intervals = %+v, want one open UP interval", cancelled)
			}
		})
	}
}

func TestStorageConformanceMaintenanceAndStateCleanup(t *testing.T) {
	for _, backend := range conformanceBackends() {
		t.Run(backend.name, func(t *testing.T) {
			store := backend.open(t)
			defer store.Close()

			ctx := context.Background()
			now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
			saveConformanceState(t, store, "active", now)
			saveConformanceState(t, store, "stale", now)

			if err := store.DeleteStatesExcept(ctx, []string{"active"}); err != nil {
				t.Fatal(err)
			}
			states, err := store.ListStates(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(states) != 1 || states[0].MonitorID != "active" {
				t.Fatalf("states = %+v, want only active", states)
			}

			id, err := store.AddMaintenanceWindow(ctx, MaintenanceWindow{
				MonitorID: "active",
				StartsAt:  now,
				EndsAt:    now.Add(time.Hour),
				Reason:    "deploy",
				CreatedBy: "tester",
				CreatedAt: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if id == 0 {
				t.Fatal("maintenance id = 0, want nonzero")
			}

			if _, err := store.AddMaintenanceWindow(ctx, MaintenanceWindow{
				MonitorID: "active",
				StartsAt:  now.Add(30 * time.Minute),
				EndsAt:    now.Add(90 * time.Minute),
				Reason:    "overlap",
				CreatedBy: "tester",
				CreatedAt: now,
			}); err == nil {
				t.Fatal("overlapping maintenance window succeeded, want error")
			}

			active, ok, err := store.ActiveMaintenanceWindow(ctx, "active", now)
			if err != nil {
				t.Fatal(err)
			}
			if !ok || active.ID != id {
				t.Fatalf("active window = %+v ok=%t, want id %d", active, ok, id)
			}
			_, ok, err = store.ActiveMaintenanceWindow(ctx, "active", now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("window active at exclusive end, want inactive")
			}

			if err := store.CancelMaintenanceWindow(ctx, id, now.Add(10*time.Minute), "tester", "done"); err != nil {
				t.Fatal(err)
			}
			windows, err := store.ListMaintenanceWindows(ctx, MaintenanceWindowFilter{MonitorID: "active", IncludeAll: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(windows) != 1 || windows[0].ID != id || windows[0].CancelledAt.IsZero() {
				t.Fatalf("maintenance windows = %+v, want cancelled window", windows)
			}
		})
	}
}

func TestStorageConformanceObserverAndAlertRetries(t *testing.T) {
	for _, backend := range conformanceBackends() {
		t.Run(backend.name, func(t *testing.T) {
			store := backend.open(t)
			defer store.Close()

			ctx := context.Background()
			now := time.Date(2026, 6, 23, 2, 0, 0, 0, time.UTC)
			incidentID, err := store.SaveObserverCheck(ctx, ObserverState{
				Status:               state.ObserverDown,
				ConsecutiveFailures:  3,
				ConsecutiveSuccesses: 0,
				LastCheckedAt:        now,
				LastFailureAt:        now,
				LastError:            "sentinel failed",
				UpdatedAt:            now,
			}, []ObserverSentinelResult{{
				SentinelID:         "edge",
				Name:               "Edge",
				URL:                "https://example.com/health",
				ExpectedStatusCode: 200,
				OK:                 false,
				ObservedStatusCode: 500,
				LatencyMS:          20,
				Error:              "bad status",
				CheckedAt:          now,
			}}, &Incident{
				MonitorID:  "__observer__",
				Name:       "Observer Connectivity",
				Transition: state.ObserverDown,
				ObservedAt: now,
				Error:      "sentinel failed",
			})
			if err != nil {
				t.Fatal(err)
			}

			observerState, ok, err := store.GetObserverState(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !ok || observerState.Status != state.ObserverDown || observerState.ConsecutiveFailures != 3 {
				t.Fatalf("observer state = %+v ok=%t, want down", observerState, ok)
			}
			sentinels, err := store.ListObserverSentinelResults(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(sentinels) != 1 || sentinels[0].SentinelID != "edge" || sentinels[0].OK {
				t.Fatalf("sentinels = %+v, want failed edge sentinel", sentinels)
			}

			if err := store.SaveAlertNotifications(ctx, []AlertNotification{
				{
					IncidentID:    incidentID,
					MonitorID:     "__observer__",
					Provider:      "smtp",
					AttemptedAt:   now,
					AttemptNumber: 1,
					Success:       false,
					Error:         "temporary",
					NextRetryAt:   now.Add(-time.Minute),
				},
				{
					IncidentID:    incidentID,
					MonitorID:     "__observer__",
					Provider:      "mailtrap",
					AttemptedAt:   now,
					AttemptNumber: 1,
					Success:       true,
				},
			}); err != nil {
				t.Fatal(err)
			}
			retries, err := store.ListDueAlertNotificationRetries(ctx, now, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(retries) != 1 || retries[0].Notification.Provider != "smtp" || retries[0].CurrentState.Status != state.ObserverDown {
				t.Fatalf("retries = %+v, want one smtp observer retry", retries)
			}
			failures, err := store.ListActionableAlertDeliveryFailures(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(failures) != 1 || failures[0].Provider != "smtp" {
				t.Fatalf("actionable failures = %+v, want smtp failure", failures)
			}
		})
	}
}

func TestStorageConformanceUptimeStatsMaintenanceSuppressionAndRollups(t *testing.T) {
	for _, backend := range conformanceBackends() {
		t.Run(backend.name, func(t *testing.T) {
			store := backend.open(t)
			defer store.Close()

			ctx := context.Background()
			now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
			saveConformanceState(t, store, "home", now.Add(-48*time.Hour))
			maintenanceID, err := store.AddMaintenanceWindow(ctx, MaintenanceWindow{
				MonitorID: "home",
				StartsAt:  now.Add(-2 * time.Hour),
				EndsAt:    now.Add(-time.Hour),
				Reason:    "deploy",
				CreatedBy: "tester",
				CreatedAt: now.Add(-3 * time.Hour),
			})
			if err != nil {
				t.Fatal(err)
			}

			for _, probe := range []ProbeResult{
				{MonitorID: "home", CheckedAt: now.Add(-26 * time.Hour), OK: false},
				{MonitorID: "home", CheckedAt: now.Add(-26*time.Hour + time.Minute), OK: false},
				{MonitorID: "home", CheckedAt: now.Add(-26*time.Hour + 2*time.Minute), OK: false},
				{MonitorID: "home", CheckedAt: now.Add(-26*time.Hour + 4*time.Minute), OK: true},
				{MonitorID: "home", CheckedAt: now.Add(-90 * time.Minute), OK: false, Error: "maintenance failure", MaintenanceWindowID: maintenanceID},
				{MonitorID: "home", CheckedAt: now.Add(-45 * time.Minute), OK: false, Error: "observer suppressed", ObserverSuppressed: true},
				{MonitorID: "home", CheckedAt: now.Add(-30 * time.Minute), OK: true},
			} {
				status := state.Down
				if probe.OK {
					status = state.Up
				}
				_, err := store.SaveProbeAndState(ctx, probe, MonitorState{
					MonitorID:     probe.MonitorID,
					Name:          "Home",
					URL:           "https://example.com/",
					Status:        status,
					LastCheckedAt: probe.CheckedAt,
					UpdatedAt:     probe.CheckedAt,
				}, nil)
				if err != nil {
					t.Fatal(err)
				}
			}

			if err := store.RollupAndPruneProbeResults(ctx, ProbeRetentionPolicy{
				ProbeResults:       RollupRetention{Duration: 24 * time.Hour},
				ProbeMinuteRollups: RollupRetention{Duration: 30 * 24 * time.Hour},
				ProbeHourlyRollups: RollupRetention{Duration: 365 * 24 * time.Hour},
				ProbeDailyRollups:  RollupRetention{Forever: true},
			}, now); err != nil {
				t.Fatal(err)
			}

			stats, err := store.ListUptimeStats(ctx, now, SingleFailureThreshold(3))
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
				t.Fatalf("downtime_seconds = %d, want 240", retained.DowntimeSeconds)
			}

			incidents, err := store.ListIncidents(ctx, IncidentFilter{Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			for _, incident := range incidents {
				if incident.Error == "maintenance failure" || incident.Error == "observer suppressed" {
					t.Fatalf("incidents = %+v, want maintenance and observer-suppressed failures excluded", incidents)
				}
			}
		})
	}
}

func TestStorageConformanceDailyUptimeSplitsConfirmedOutageAcrossUTCDays(t *testing.T) {
	for _, backend := range conformanceBackends() {
		t.Run(backend.name, func(t *testing.T) {
			store := backend.open(t)
			defer store.Close()

			ctx := context.Background()
			now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
			steps := []struct {
				at     time.Time
				status string
				ok     bool
			}{
				{at: time.Date(2026, 7, 14, 6, 0, 0, 0, time.UTC), status: state.Up, ok: true},
				{at: time.Date(2026, 7, 14, 23, 0, 0, 0, time.UTC), status: state.Failing, ok: false},
				{at: time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC), status: state.Down, ok: false},
				{at: time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC), status: state.Up, ok: true},
			}
			for _, step := range steps {
				_, err := store.SaveProbeAndState(ctx, ProbeResult{
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

			daily, err := store.ListDailyUptimeStats(ctx, now, 3, SingleFailureThreshold(2))
			if err != nil {
				t.Fatal(err)
			}
			home := daily["home"]
			if len(home) != 3 {
				t.Fatalf("daily entries = %d, want 3", len(home))
			}
			assertDailyUptime(t, home[0], "2026-07-14", 18*60*60, 60*60)
			assertDailyUptime(t, home[1], "2026-07-15", 24*60*60, 60*60)
			assertDailyUptime(t, home[2], "2026-07-16", 12*60*60, 0)
		})
	}
}

func TestStorageConformanceFailedProbesAndSentinelEvents(t *testing.T) {
	for _, backend := range conformanceBackends() {
		t.Run(backend.name, func(t *testing.T) {
			store := backend.open(t)
			defer store.Close()

			ctx := context.Background()
			now := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)

			_, err := store.SaveProbeAndState(ctx, ProbeResult{
				MonitorID:          "web",
				CheckedAt:          now.Add(-3 * time.Minute),
				OK:                 false,
				ObservedStatusCode: 503,
				Error:              "timeout",
			}, MonitorState{
				MonitorID:              "web",
				Name:                   "Web",
				URL:                    "https://example.com",
				Status:                 state.Failing,
				LastCheckedAt:          now.Add(-3 * time.Minute),
				LastFailureAt:          now.Add(-3 * time.Minute),
				LastError:              "timeout",
				LastObservedStatusCode: 503,
				UpdatedAt:              now.Add(-3 * time.Minute),
			}, nil)
			if err != nil {
				t.Fatal(err)
			}

			_, err = store.SaveProbeAndState(ctx, ProbeResult{
				MonitorID:          "web",
				CheckedAt:          now.Add(-2 * time.Minute),
				OK:                 false,
				ObservedStatusCode: 503,
				Error:              "timeout",
				ObserverSuppressed: true,
			}, MonitorState{
				MonitorID:              "web",
				Name:                   "Web",
				URL:                    "https://example.com",
				Status:                 state.Failing,
				LastCheckedAt:          now.Add(-2 * time.Minute),
				LastFailureAt:          now.Add(-2 * time.Minute),
				LastError:              "timeout",
				LastObservedStatusCode: 503,
				UpdatedAt:              now.Add(-2 * time.Minute),
			}, nil)
			if err != nil {
				t.Fatal(err)
			}

			_, err = store.SaveProbeAndState(ctx, ProbeResult{
				MonitorID: "web",
				CheckedAt: now.Add(-time.Minute),
				OK:        true,
			}, MonitorState{
				MonitorID:              "web",
				Name:                   "Web",
				URL:                    "https://example.com",
				Status:                 state.Up,
				LastCheckedAt:          now.Add(-time.Minute),
				LastSuccessAt:          now.Add(-time.Minute),
				LastObservedStatusCode: 200,
				UpdatedAt:              now.Add(-time.Minute),
			}, nil)
			if err != nil {
				t.Fatal(err)
			}

			failedProbes, err := store.ListFailedProbeResults(ctx, ProbeResultFilter{Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(failedProbes) != 2 {
				t.Fatalf("ListFailedProbeResults = %d, want 2 (got %+v)", len(failedProbes), failedProbes)
			}
			if !failedProbes[0].ObserverSuppressed && failedProbes[1].ObserverSuppressed {
				failedProbes[0], failedProbes[1] = failedProbes[1], failedProbes[0]
			}
			if !failedProbes[0].ObserverSuppressed || failedProbes[0].MonitorID != "web" {
				t.Fatalf("first failed probe = %+v, want suppressed web probe", failedProbes[0])
			}
			if failedProbes[1].ObserverSuppressed || failedProbes[1].MonitorID != "web" || failedProbes[1].Error != "timeout" {
				t.Fatalf("second failed probe = %+v, want non-suppressed web probe with timeout", failedProbes[1])
			}

			recentFailedProbes, err := store.ListFailedProbeResults(ctx, ProbeResultFilter{Limit: 10, Since: now.Add(-2 * time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			if len(recentFailedProbes) != 1 || !recentFailedProbes[0].ObserverSuppressed {
				t.Fatalf("recent failed probes = %+v, want only suppressed probe at cutoff", recentFailedProbes)
			}

			_, err = store.SaveObserverCheck(ctx, ObserverState{
				Status:               state.ObserverDown,
				ConsecutiveFailures:  1,
				ConsecutiveSuccesses: 0,
				LastCheckedAt:        now.Add(-3 * time.Minute),
				LastFailureAt:        now.Add(-3 * time.Minute),
				LastError:            "old sentinel failed",
				UpdatedAt:            now.Add(-3 * time.Minute),
			}, []ObserverSentinelResult{
				{
					SentinelID:         "old",
					Name:               "Old",
					URL:                "https://old.example.com",
					ExpectedStatusCode: 204,
					OK:                 false,
					ObservedStatusCode: 0,
					LatencyMS:          500,
					Error:              "old connection refused",
					CheckedAt:          now.Add(-3 * time.Minute),
				},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}

			incidentID, err := store.SaveObserverCheck(ctx, ObserverState{
				Status:               state.ObserverDown,
				ConsecutiveFailures:  2,
				ConsecutiveSuccesses: 0,
				LastCheckedAt:        now,
				LastFailureAt:        now,
				LastError:            "sentinel failed",
				UpdatedAt:            now,
			}, []ObserverSentinelResult{
				{
					SentinelID:         "gstatic",
					Name:               "Google",
					URL:                "https://gstatic.com/generate_204",
					ExpectedStatusCode: 204,
					OK:                 false,
					ObservedStatusCode: 0,
					LatencyMS:          500,
					Error:              "connection refused",
					CheckedAt:          now,
				},
				{
					SentinelID:         "cloudflare",
					Name:               "Cloudflare",
					URL:                "https://cp.cloudflare.com/generate_204",
					ExpectedStatusCode: 204,
					OK:                 true,
					ObservedStatusCode: 204,
					LatencyMS:          20,
					CheckedAt:          now,
				},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			_ = incidentID

			sentinelEvents, err := store.ListObserverSentinelEvents(ctx, ObserverSentinelEventFilter{Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(sentinelEvents) != 2 {
				t.Fatalf("ListObserverSentinelEvents = %d, want 2 failed sentinels", len(sentinelEvents))
			}
			if sentinelEvents[0].SentinelID != "gstatic" || sentinelEvents[0].OK {
				t.Fatalf("sentinel event = %+v, want failed gstatic", sentinelEvents[0])
			}
			recentSentinelEvents, err := store.ListObserverSentinelEvents(ctx, ObserverSentinelEventFilter{Limit: 10, Since: now.Add(-time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			if len(recentSentinelEvents) != 1 || recentSentinelEvents[0].SentinelID != "gstatic" {
				t.Fatalf("recent sentinel events = %+v, want only gstatic", recentSentinelEvents)
			}
		})
	}
}

func saveConformanceState(t *testing.T, store Backend, monitorID string, now time.Time) {
	t.Helper()
	_, err := store.SaveProbeAndState(context.Background(), ProbeResult{
		MonitorID:          monitorID,
		CheckedAt:          now,
		OK:                 true,
		ObservedStatusCode: 200,
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
