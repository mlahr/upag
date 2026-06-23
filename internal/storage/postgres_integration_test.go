package storage

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var postgresTestDSN string

func TestMain(m *testing.M) {
	code := runPostgresTests(m)
	os.Exit(code)
}

func runPostgresTests(m *testing.M) int {
	containerName := fmt.Sprintf("upag-postgres-test-%d", time.Now().UnixNano())
	run := exec.Command("docker", "run", "-d", "--rm",
		"--name", containerName,
		"-e", "POSTGRES_PASSWORD=postgres",
		"-e", "POSTGRES_DB=upag_test",
		"-p", "127.0.0.1::5432",
		"postgres:16-alpine",
	)
	output, err := run.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres test container: %v\n%s", err, output)
		return 1
	}
	defer func() {
		_ = exec.Command("docker", "rm", "-f", containerName).Run()
	}()

	portOutput, err := exec.Command("docker", "port", containerName, "5432/tcp").CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "inspect postgres test container port: %v\n%s", err, portOutput)
		return 1
	}
	hostPort := strings.TrimSpace(string(portOutput))
	_, port, ok := strings.Cut(hostPort, ":")
	if !ok || port == "" {
		fmt.Fprintf(os.Stderr, "unexpected docker port output %q\n", hostPort)
		return 1
	}
	postgresTestDSN = fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%s/upag_test?sslmode=disable", port)

	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		store, err := OpenPostgres(ctx, postgresTestDSN)
		cancel()
		if err == nil {
			_ = store.Close()
			break
		}
		if time.Now().After(deadline) {
			fmt.Fprintf(os.Stderr, "postgres test container did not become ready: %v\n", err)
			return 1
		}
		time.Sleep(500 * time.Millisecond)
	}

	return m.Run()
}

func TestPostgresStoreCoreBehavior(t *testing.T) {
	ctx := context.Background()
	store, err := OpenPostgres(ctx, postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resetPostgresTestData(t, store)

	now := time.Date(2026, 6, 23, 1, 0, 0, 0, time.UTC)
	incidentID, err := store.SaveProbeAndState(ctx, ProbeResult{
		MonitorID:          "home",
		CheckedAt:          now,
		OK:                 false,
		ObservedStatusCode: 503,
		LatencyMS:          120,
		ResponseTimeMS:     125,
		AttemptCount:       3,
		Error:              "expected HTTP status 200, observed HTTP status 503",
	}, MonitorState{
		MonitorID:              "home",
		Name:                   "Home",
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 "DOWN",
		ConsecutiveFailures:    3,
		LastCheckedAt:          now,
		LastFailureAt:          now,
		LastError:              "expected HTTP status 200, observed HTTP status 503",
		LastObservedStatusCode: 503,
		UpdatedAt:              now,
	}, &Incident{
		MonitorID:  "home",
		Name:       "Home",
		Transition: "DOWN",
		ObservedAt: now,
		Error:      "expected HTTP status 200, observed HTTP status 503",
		StatusCode: 503,
	})
	if err != nil {
		t.Fatal(err)
	}
	if incidentID == 0 {
		t.Fatal("incidentID = 0, want inserted incident id")
	}

	state, ok, err := store.GetState(ctx, "home")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || state.Status != "DOWN" || state.LastObservedStatusCode != 503 {
		t.Fatalf("state = %+v ok=%t, want DOWN with status code 503", state, ok)
	}

	incidents, err := store.ListIncidents(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 1 || incidents[0].Transition != "DOWN" {
		t.Fatalf("incidents = %+v, want DOWN incident", incidents)
	}

	maintenanceID, err := store.AddMaintenanceWindow(ctx, MaintenanceWindow{
		MonitorID: "home",
		StartsAt:  now.Add(time.Hour),
		EndsAt:    now.Add(2 * time.Hour),
		Reason:    "deploy",
		CreatedBy: "tester",
		CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CancelMaintenanceWindow(ctx, maintenanceID, now.Add(30*time.Minute), "tester", "done"); err != nil {
		t.Fatal(err)
	}
	windows, err := store.ListMaintenanceWindows(ctx, MaintenanceWindowFilter{IncludeAll: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(windows) != 1 || windows[0].CancelledAt.IsZero() {
		t.Fatalf("windows = %+v, want cancelled maintenance window", windows)
	}
}

func TestPostgresStoreObserverAlertsAndRollups(t *testing.T) {
	ctx := context.Background()
	store, err := OpenPostgres(ctx, postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resetPostgresTestData(t, store)

	now := time.Date(2026, 6, 23, 2, 0, 0, 0, time.UTC)
	incidentID, err := store.SaveObserverCheck(ctx, ObserverState{
		Status:               "OBSERVER_DOWN",
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
		Transition: "OBSERVER_DOWN",
		ObservedAt: now,
		Error:      "sentinel failed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if incidentID == 0 {
		t.Fatal("observer incidentID = 0")
	}

	if err := store.SaveAlertNotifications(ctx, []AlertNotification{{
		IncidentID:    incidentID,
		MonitorID:     "__observer__",
		Provider:      "smtp",
		AttemptedAt:   now,
		AttemptNumber: 1,
		Success:       false,
		Error:         "temporary",
		NextRetryAt:   now.Add(-time.Minute),
	}}); err != nil {
		t.Fatal(err)
	}
	retries, err := store.ListDueAlertNotificationRetries(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(retries) != 1 || retries[0].CurrentState.Status != "OBSERVER_DOWN" {
		t.Fatalf("retries = %+v, want observer retry", retries)
	}

	if _, err := store.SaveProbeAndState(ctx, ProbeResult{
		MonitorID: "home",
		CheckedAt: now.Add(-2 * time.Hour),
		OK:        true,
	}, MonitorState{
		MonitorID:              "home",
		Name:                   "Home",
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 "UP",
		LastCheckedAt:          now.Add(-2 * time.Hour),
		LastSuccessAt:          now.Add(-2 * time.Hour),
		LastObservedStatusCode: 200,
		UpdatedAt:              now.Add(-2 * time.Hour),
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.RollupAndPruneProbeResults(ctx, ProbeRetentionPolicy{
		ProbeResults:       RollupRetention{Duration: time.Hour},
		ProbeMinuteRollups: RollupRetention{Duration: 24 * time.Hour},
		ProbeHourlyRollups: RollupRetention{Duration: 7 * 24 * time.Hour},
		ProbeDailyRollups:  RollupRetention{Forever: true},
	}, now); err != nil {
		t.Fatal(err)
	}
	stats, err := store.ListUptimeStats(ctx, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if stats["home"].Retained.TotalChecks != 1 {
		t.Fatalf("home retained stats = %+v, want one check", stats["home"].Retained)
	}
}

func TestMigrateSQLiteToPostgres(t *testing.T) {
	ctx := context.Background()
	target, err := OpenPostgres(ctx, postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	resetPostgresTestData(t, target)
	target.Close()

	sqlitePath := filepath.Join(t.TempDir(), "source.sqlite")
	source, err := Open(sqlitePath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 6, 23, 3, 0, 0, 0, time.UTC)
	incidentID, err := source.SaveProbeAndState(ctx, ProbeResult{
		MonitorID: "home",
		CheckedAt: now,
		OK:        false,
		Error:     "down",
	}, MonitorState{
		MonitorID:          "home",
		Name:               "Home",
		URL:                "https://example.com/",
		ExpectedStatusCode: 200,
		Status:             "DOWN",
		LastCheckedAt:      now,
		LastFailureAt:      now,
		LastError:          "down",
		UpdatedAt:          now,
	}, &Incident{
		MonitorID:  "home",
		Name:       "Home",
		Transition: "DOWN",
		ObservedAt: now,
		Error:      "down",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.SaveAlertNotifications(ctx, []AlertNotification{{
		IncidentID:    incidentID,
		MonitorID:     "home",
		Provider:      "smtp",
		AttemptedAt:   now,
		Success:       true,
		NextRetryAt:   time.Time{},
		AttemptNumber: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	source.Close()

	if err := MigrateSQLiteToPostgres(ctx, sqlitePath, postgresTestDSN); err != nil {
		t.Fatal(err)
	}
	migrated, err := OpenPostgres(ctx, postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	state, ok, err := migrated.GetState(ctx, "home")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || state.Status != "DOWN" {
		t.Fatalf("migrated state = %+v ok=%t, want DOWN", state, ok)
	}
	notifications, err := migrated.ListAlertNotifications(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(notifications) != 1 || !notifications[0].Success {
		t.Fatalf("migrated notifications = %+v, want one successful notification", notifications)
	}
}

func resetPostgresTestData(t *testing.T, store *PostgresStore) {
	t.Helper()
	_, err := store.pool.Exec(context.Background(), `TRUNCATE
		alert_notifications,
		incidents,
		probe_results,
		probe_minute_rollups,
		probe_hourly_rollups,
		probe_daily_rollups,
		probe_outcome_runs,
		maintenance_windows,
		monitor_states,
		observer_state,
		observer_sentinel_results
		RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
}
