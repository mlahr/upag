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

			incidents, err := store.ListIncidents(ctx, 10)
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

			incidents, err := store.ListIncidents(ctx, 10)
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

			failedProbes, err := store.ListFailedProbeResults(ctx, 10)
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

			sentinelEvents, err := store.ListObserverSentinelEvents(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(sentinelEvents) != 1 {
				t.Fatalf("ListObserverSentinelEvents = %d, want 1 (only failed sentinel)", len(sentinelEvents))
			}
			if sentinelEvents[0].SentinelID != "gstatic" || sentinelEvents[0].OK {
				t.Fatalf("sentinel event = %+v, want failed gstatic", sentinelEvents[0])
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
