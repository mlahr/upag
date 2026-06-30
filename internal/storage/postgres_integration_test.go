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

	"upag/internal/state"
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

func TestPostgresRollupGroupByTenantColumnOrdering(t *testing.T) {
	ctx := context.Background()
	store, err := OpenPostgres(ctx, postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resetPostgresTestData(t, store)

	now := time.Date(2026, 6, 23, 5, 0, 0, 0, time.UTC)
	tenantID := "tenant-rollup"

	if _, err := store.SaveProbeAndState(WithTenant(ctx, tenantID), ProbeResult{
		MonitorID: "shared",
		CheckedAt: now.Add(-2 * time.Hour),
		OK:        true,
	}, MonitorState{
		MonitorID:              "shared",
		Name:                   "Tenant Rollup Monitor",
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

	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := postgresRollupRawProbeResults(ctx, tx, tenantID, "probe_minute_rollups", postgresMinuteBucketExpression("checked_at"), now.Add(-time.Hour)); err != nil {
		t.Fatalf("raw rollup query: %v", err)
	}
	if err := postgresRollupStoredProbeRollups(ctx, tx, tenantID, "probe_minute_rollups", "probe_hourly_rollups", postgresHourlyBucketExpression("bucket_start"), now.Add(-24*time.Hour)); err != nil {
		t.Fatalf("stored rollup query: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	stats, err := store.ListUptimeStats(WithTenant(ctx, tenantID), now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stats["shared"]; !ok {
		t.Fatalf("tenant-scoped rollup stats missing for shared monitor")
	}
	defaultStats, err := store.ListUptimeStats(ctx, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := defaultStats["shared"]; ok {
		t.Fatalf("default tenant unexpectedly has rollup stats for tenant-scoped monitor")
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

	if err := MigrateSQLiteToPostgres(ctx, sqlitePath, postgresTestDSN, defaultTenantID); err != nil {
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

func TestPostgresStoreTenantIsolation(t *testing.T) {
	ctx := context.Background()
	store, err := OpenPostgres(ctx, postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resetPostgresTestData(t, store)

	now := time.Date(2026, 6, 23, 4, 0, 0, 0, time.UTC)
	tenantA := "tenant-a"
	tenantB := "tenant-b"

	aIncidentID, err := store.SaveProbeAndState(WithTenant(ctx, tenantA), ProbeResult{
		MonitorID:          "shared",
		CheckedAt:          now,
		OK:                 false,
		ObservedStatusCode: 503,
		LatencyMS:          200,
		ResponseTimeMS:     210,
		Error:              "timeout",
	}, MonitorState{
		MonitorID:              "shared",
		Name:                   "Tenant A shared",
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 "DOWN",
		ConsecutiveFailures:    1,
		LastCheckedAt:          now,
		LastFailureAt:          now,
		LastError:              "timeout",
		LastObservedStatusCode: 503,
		UpdatedAt:              now,
	}, &Incident{
		MonitorID:  "shared",
		Name:       "Tenant A shared",
		Transition: state.Down,
		ObservedAt: now,
		Error:      "timeout",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.SaveProbeAndState(WithTenant(ctx, tenantA), ProbeResult{
		MonitorID: "stale",
		CheckedAt: now.Add(time.Minute),
		OK:        true,
	}, MonitorState{
		MonitorID:              "stale",
		Name:                   "Tenant A stale",
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 "UP",
		ConsecutiveFailures:    0,
		LastCheckedAt:          now.Add(time.Minute),
		LastSuccessAt:          now.Add(time.Minute),
		LastObservedStatusCode: 200,
		UpdatedAt:              now.Add(time.Minute),
	}, nil); err != nil {
		t.Fatal(err)
	}
	if aIncidentID == 0 {
		t.Fatalf("incident id = %d, want nonzero", aIncidentID)
	}

	bIncidentID, err := store.SaveProbeAndState(WithTenant(ctx, tenantB), ProbeResult{
		MonitorID:          "shared",
		CheckedAt:          now,
		OK:                 false,
		ObservedStatusCode: 502,
		Error:              "down",
	}, MonitorState{
		MonitorID:              "shared",
		Name:                   "Tenant B shared",
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 "DOWN",
		ConsecutiveFailures:    1,
		LastCheckedAt:          now,
		LastFailureAt:          now,
		LastError:              "down",
		LastObservedStatusCode: 502,
		UpdatedAt:              now,
	}, &Incident{
		MonitorID:  "shared",
		Name:       "Tenant B shared",
		Transition: state.Down,
		ObservedAt: now,
		Error:      "down",
	})
	if err != nil {
		t.Fatal(err)
	}
	if bIncidentID == 0 {
		t.Fatalf("incident id = %d, want nonzero", bIncidentID)
	}

	aWindowID, err := store.AddMaintenanceWindow(WithTenant(ctx, tenantA), MaintenanceWindow{
		MonitorID: "shared",
		StartsAt:  now.Add(-time.Minute),
		EndsAt:    now.Add(time.Hour),
		Reason:    "deploy",
		CreatedBy: "tenant-a",
		CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	bWindowID, err := store.AddMaintenanceWindow(WithTenant(ctx, tenantB), MaintenanceWindow{
		MonitorID: "shared",
		StartsAt:  now.Add(-time.Minute),
		EndsAt:    now.Add(time.Hour),
		Reason:    "deploy",
		CreatedBy: "tenant-b",
		CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if aWindowID == 0 || bWindowID == 0 {
		t.Fatalf("window ids = %d and %d, want nonzero", aWindowID, bWindowID)
	}

	if err := store.SaveAlertNotifications(WithTenant(ctx, tenantA), []AlertNotification{
		{
			IncidentID:    aIncidentID,
			MonitorID:     "shared",
			Provider:      "smtp",
			AttemptedAt:   now,
			AttemptNumber: 1,
			Success:       false,
			Error:         "temporary",
			NextRetryAt:   now.Add(5 * time.Minute),
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveAlertNotifications(WithTenant(ctx, tenantB), []AlertNotification{
		{
			IncidentID:    bIncidentID,
			MonitorID:     "shared",
			Provider:      "smtp",
			AttemptedAt:   now,
			AttemptNumber: 1,
			Success:       false,
			Error:         "temporary",
			NextRetryAt:   now.Add(5 * time.Minute),
		},
	}); err != nil {
		t.Fatal(err)
	}

	stateA, ok, err := store.GetState(WithTenant(ctx, tenantA), "shared")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || stateA.Name != "Tenant A shared" {
		t.Fatalf("tenant A state = %+v ok=%t, want Tenant A shared", stateA, ok)
	}
	stateB, ok, err := store.GetState(WithTenant(ctx, tenantB), "shared")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || stateB.Name != "Tenant B shared" {
		t.Fatalf("tenant B state = %+v ok=%t, want Tenant B shared", stateB, ok)
	}
	stateDefault, ok, err := store.GetState(ctx, "shared")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("default tenant state = %+v, want missing", stateDefault)
	}

	tenA, err := store.ListStates(WithTenant(ctx, tenantA))
	if err != nil {
		t.Fatal(err)
	}
	if len(tenA) != 2 {
		t.Fatalf("tenant A state count = %d, want 2", len(tenA))
	}
	tenB, err := store.ListStates(WithTenant(ctx, tenantB))
	if err != nil {
		t.Fatal(err)
	}
	if len(tenB) != 1 {
		t.Fatalf("tenant B state count = %d, want 1", len(tenB))
	}

	windowA, err := store.ListMaintenanceWindows(WithTenant(ctx, tenantA), MaintenanceWindowFilter{MonitorID: "shared", IncludeAll: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(windowA) != 1 || windowA[0].ID != aWindowID {
		t.Fatalf("tenant A windows = %+v, want one window %d", windowA, aWindowID)
	}
	windowB, err := store.ListMaintenanceWindows(WithTenant(ctx, tenantB), MaintenanceWindowFilter{MonitorID: "shared", IncludeAll: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(windowB) != 1 || windowB[0].ID != bWindowID {
		t.Fatalf("tenant B windows = %+v, want one window %d", windowB, bWindowID)
	}

	incidentsA, err := store.ListIncidents(WithTenant(ctx, tenantA), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(incidentsA) != 1 || incidentsA[0].MonitorID != "shared" || incidentsA[0].Transition != "DOWN" {
		t.Fatalf("tenant A incidents = %+v, want one down incident for shared", incidentsA)
	}
	incidentsB, err := store.ListIncidents(WithTenant(ctx, tenantB), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(incidentsB) != 1 || incidentsB[0].MonitorID != "shared" || incidentsB[0].Transition != "DOWN" {
		t.Fatalf("tenant B incidents = %+v, want one down incident for shared", incidentsB)
	}

	failuresA, err := store.ListActionableAlertDeliveryFailures(WithTenant(ctx, tenantA), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(failuresA) != 1 || failuresA[0].MonitorID != "shared" || failuresA[0].Provider != "smtp" {
		t.Fatalf("tenant A failures = %+v, want one smtp failure", failuresA)
	}
	failuresB, err := store.ListActionableAlertDeliveryFailures(WithTenant(ctx, tenantB), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(failuresB) != 1 || failuresB[0].MonitorID != "shared" || failuresB[0].Provider != "smtp" {
		t.Fatalf("tenant B failures = %+v, want one smtp failure", failuresB)
	}

	if err := store.DeleteStatesExcept(WithTenant(ctx, tenantA), []string{"shared"}); err != nil {
		t.Fatal(err)
	}
	afterTenantA, err := store.ListStates(WithTenant(ctx, tenantA))
	if err != nil {
		t.Fatal(err)
	}
	if len(afterTenantA) != 1 || afterTenantA[0].MonitorID != "shared" {
		t.Fatalf("tenant A states after cleanup = %+v, want one shared state", afterTenantA)
	}
	afterTenantB, err := store.ListStates(WithTenant(ctx, tenantB))
	if err != nil {
		t.Fatal(err)
	}
	if len(afterTenantB) != 1 {
		t.Fatalf("tenant B states after tenant A cleanup = %d, want 1", len(afterTenantB))
	}
}

func TestMigrateSQLiteToPostgresDefaultsTenantID(t *testing.T) {
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
	_, err = source.SaveProbeAndState(ctx, ProbeResult{
		MonitorID: "tenantless",
		CheckedAt: now,
		OK:        false,
		Error:     "down",
	}, MonitorState{
		MonitorID:              "tenantless",
		Name:                   "Tenantless",
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 "DOWN",
		LastCheckedAt:          now,
		LastFailureAt:          now,
		LastError:              "down",
		LastObservedStatusCode: 0,
		UpdatedAt:              now,
	}, &Incident{
		MonitorID:  "tenantless",
		Name:       "Tenantless",
		Transition: "DOWN",
		ObservedAt: now,
		Error:      "down",
	})
	if err != nil {
		t.Fatal(err)
	}
	source.Close()

	if err := MigrateSQLiteToPostgres(ctx, sqlitePath, postgresTestDSN, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	migrated, err := OpenPostgres(ctx, postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()

	state, ok, err := migrated.GetState(ctx, "tenantless")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || state.Status != "DOWN" {
		t.Fatalf("default tenant state = %+v ok=%t, want DOWN", state, ok)
	}

	stateTenant, ok, err := migrated.GetState(WithTenant(ctx, "team-custom"), "tenantless")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("custom tenant state = %+v, want missing", stateTenant)
	}
}

func TestMigrateSQLiteToPostgresRejectsInvalidTenantID(t *testing.T) {
	ctx := context.Background()
	target, err := OpenPostgres(ctx, postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	resetPostgresTestData(t, target)
	target.Close()

	sqlitePath := filepath.Join(t.TempDir(), "source.sqlite")
	emptySource, err := Open(sqlitePath)
	if err != nil {
		t.Fatal(err)
	}
	emptySource.Close()

	if err := MigrateSQLiteToPostgres(ctx, sqlitePath, postgresTestDSN, "tenant with space"); err == nil {
		t.Fatal("expected tenant validation error")
	}
	if err := MigrateSQLiteToPostgres(ctx, sqlitePath, postgresTestDSN, "   "); err == nil {
		t.Fatal("expected tenant validation error for whitespace tenant_id")
	}
	if err := MigrateSQLiteToPostgres(ctx, sqlitePath, postgresTestDSN, " tenant"); err == nil {
		t.Fatal("expected tenant validation error for leading whitespace in tenant_id")
	}
	if err := MigrateSQLiteToPostgres(ctx, sqlitePath, postgresTestDSN, "tenant "); err == nil {
		t.Fatal("expected tenant validation error for trailing whitespace in tenant_id")
	}
	if err := MigrateSQLiteToPostgres(ctx, sqlitePath, postgresTestDSN, "ténant"); err == nil {
		t.Fatal("expected tenant validation error for unicode tenant_id")
	}
}

func TestMigrateSQLiteToPostgresUsesExplicitTenantID(t *testing.T) {
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
	now := time.Date(2026, 6, 23, 4, 0, 0, 0, time.UTC)
	tenantID := "tenant-migrated"
	_, err = source.SaveProbeAndState(ctx, ProbeResult{
		MonitorID: "tenant-specific",
		CheckedAt: now,
		OK:        false,
		Error:     "down",
	}, MonitorState{
		MonitorID:              "tenant-specific",
		Name:                   "Tenant specific",
		URL:                    "https://example.com/",
		ExpectedStatusCode:     200,
		Status:                 "DOWN",
		LastCheckedAt:          now,
		LastFailureAt:          now,
		LastError:              "down",
		LastObservedStatusCode: 0,
		UpdatedAt:              now,
	}, &Incident{
		MonitorID:  "tenant-specific",
		Name:       "Tenant specific",
		Transition: "DOWN",
		ObservedAt: now,
		Error:      "down",
	})
	if err != nil {
		t.Fatal(err)
	}
	source.Close()

	if err := MigrateSQLiteToPostgres(ctx, sqlitePath, postgresTestDSN, tenantID); err != nil {
		t.Fatal(err)
	}
	migrated, err := OpenPostgres(ctx, postgresTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()

	state, ok, err := migrated.GetState(WithTenant(ctx, tenantID), "tenant-specific")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || state.Status != "DOWN" {
		t.Fatalf("tenant state = %+v ok=%t, want DOWN", state, ok)
	}
	stateDefault, ok, err := migrated.GetState(ctx, "tenant-specific")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("default tenant state = %+v, want missing", stateDefault)
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
		monitor_status_intervals,
		monitor_status_interval_backfills,
		maintenance_windows,
		monitor_states,
		observer_state,
		observer_sentinel_results,
		observer_sentinel_events
		RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
}
