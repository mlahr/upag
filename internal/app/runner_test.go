package app

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"upag/internal/alert"
	"upag/internal/config"
	"upag/internal/state"
	"upag/internal/storage"
)

func TestProbeSuppressesAlertAndUptimeDuringMaintenance(t *testing.T) {
	ctx := context.Background()
	statusCode := http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	mon := config.MonitorConfig{
		ID:                 "home",
		Name:               "Home",
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		Interval:           config.Duration{Duration: time.Minute},
	}
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}
	sender := &fakeIncidentSender{}
	runner.emailer = sender

	runner.probe(ctx, 1, 0, 0, mon)
	statusCode = http.StatusServiceUnavailable
	now := time.Now().UTC()
	if _, err := store.AddMaintenanceWindow(ctx, storage.MaintenanceWindow{
		MonitorID: "home",
		StartsAt:  now.Add(-time.Hour),
		EndsAt:    now.Add(time.Hour),
		Reason:    "deploy",
		CreatedBy: "michael",
		CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	runner.probe(ctx, 1, 0, 0, mon)

	if got := sender.count(); got != 0 {
		t.Fatalf("sent alerts = %d, want 0", got)
	}
	current, ok, err := store.GetState(ctx, "home")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("state not found")
	}
	if current.Status != state.Maintenance {
		t.Fatalf("status = %s, want MAINTENANCE", current.Status)
	}
	stats, err := store.ListUptimeStats(ctx, time.Now().UTC(), storage.SingleFailureThreshold(1))
	if err != nil {
		t.Fatal(err)
	}
	retained := stats["home"].Retained
	if retained.MaintenanceFailedChecks != 1 {
		t.Fatalf("retained stats = %+v, want one failed maintenance check", retained)
	}
	incidents, err := store.ListIncidents(ctx, storage.IncidentFilter{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 0 {
		t.Fatalf("incidents = %+v, want no incident during maintenance", incidents)
	}
}

func TestProbeAlertsOnFirstFailureAfterMaintenanceEnds(t *testing.T) {
	ctx := context.Background()
	statusCode := http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	mon := config.MonitorConfig{
		ID:                 "home",
		Name:               "Home",
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		Interval:           config.Duration{Duration: time.Minute},
	}
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}
	sender := &fakeIncidentSender{}
	runner.emailer = sender

	runner.probe(ctx, 3, 0, 0, mon)
	statusCode = http.StatusServiceUnavailable
	now := time.Now().UTC()
	windowID, err := store.AddMaintenanceWindow(ctx, storage.MaintenanceWindow{
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
	runner.probe(ctx, 3, 0, 0, mon)
	if err := store.CancelMaintenanceWindow(ctx, windowID, time.Now().UTC(), "michael", "finished"); err != nil {
		t.Fatal(err)
	}

	runner.probe(ctx, 3, 0, 0, mon)

	if got := sender.count(); got != 1 {
		t.Fatalf("sent alerts = %d, want 1", got)
	}
	incidents, err := store.ListIncidents(ctx, storage.IncidentFilter{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 1 || incidents[0].Transition != state.Down {
		t.Fatalf("incidents = %+v, want one DOWN incident after maintenance", incidents)
	}
}

func TestProbeSendsRecoveryAfterPreMaintenanceDownRecoversDuringMaintenance(t *testing.T) {
	ctx := context.Background()
	statusCode := http.StatusServiceUnavailable
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	mon := config.MonitorConfig{
		ID:                 "home",
		Name:               "Home",
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		Interval:           config.Duration{Duration: time.Minute},
	}
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}
	sender := &fakeIncidentSender{}
	runner.emailer = sender

	runner.probe(ctx, 1, 0, 0, mon)
	if got := sender.count(); got != 1 {
		t.Fatalf("sent alerts after initial failure = %d, want 1", got)
	}
	now := time.Now().UTC()
	windowID, err := store.AddMaintenanceWindow(ctx, storage.MaintenanceWindow{
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
	statusCode = http.StatusOK
	runner.probe(ctx, 1, 0, 0, mon)
	if err := store.CancelMaintenanceWindow(ctx, windowID, time.Now().UTC(), "michael", "finished"); err != nil {
		t.Fatal(err)
	}

	runner.probe(ctx, 1, 0, 0, mon)

	incidents := sender.all()
	if len(incidents) != 2 || incidents[1].Transition != state.Up {
		t.Fatalf("sent incidents = %+v, want DOWN followed by UP", incidents)
	}
	current, ok, err := store.GetState(ctx, "home")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("state not found")
	}
	if current.Status != state.Up || current.StatusBeforeMaintenance != "" {
		t.Fatalf("state = %+v, want UP with no status_before_maintenance", current)
	}
}

func TestProbeRetriesBeforeRecordingSuccess(t *testing.T) {
	ctx := context.Background()
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&requests, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	mon := config.MonitorConfig{
		ID:                 "home",
		Name:               "Home",
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		Interval:           config.Duration{Duration: time.Minute},
	}
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}
	runner.emailer = &fakeIncidentSender{}

	runner.probe(ctx, 1, 2, time.Millisecond, mon)

	var ok int
	var attemptCount int
	if err := queryProbeAttempt(ctx, dbPath, "home", &ok, &attemptCount); err != nil {
		t.Fatal(err)
	}
	if ok != 1 || attemptCount != 2 {
		t.Fatalf("stored probe ok=%d attempt_count=%d, want ok=1 attempt_count=2", ok, attemptCount)
	}
}

func TestProbeRecordsFailedResultAfterRetriesExhausted(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	mon := config.MonitorConfig{
		ID:                 "home",
		Name:               "Home",
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		Interval:           config.Duration{Duration: time.Minute},
	}
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}
	runner.emailer = &fakeIncidentSender{}

	runner.probe(ctx, 3, 2, time.Millisecond, mon)

	var ok int
	var attemptCount int
	if err := queryProbeAttempt(ctx, dbPath, "home", &ok, &attemptCount); err != nil {
		t.Fatal(err)
	}
	if ok != 0 || attemptCount != 3 {
		t.Fatalf("stored probe ok=%d attempt_count=%d, want ok=0 attempt_count=3", ok, attemptCount)
	}
}

func TestProbeSuppressesMonitorTransitionWhenObserverDown(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now().UTC()
	if _, err := store.SaveObserverCheck(ctx, storage.ObserverState{
		Status:              state.ObserverDown,
		ConsecutiveFailures: 3,
		LastCheckedAt:       now,
		LastFailureAt:       now,
		LastError:           "observer timeout",
		UpdatedAt:           now,
	}, nil, nil); err != nil {
		t.Fatal(err)
	}
	mon := config.MonitorConfig{
		ID:                 "home",
		Name:               "Home",
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		Interval:           config.Duration{Duration: time.Minute},
	}
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}
	sender := &fakeIncidentSender{}
	runner.emailer = sender

	runner.probe(ctx, 1, 0, 0, mon)

	if got := sender.count(); got != 0 {
		t.Fatalf("sent alerts = %d, want 0", got)
	}
	if _, ok, err := store.GetState(ctx, "home"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("monitor state exists, want suppressed probe without state transition")
	}
	var ok int
	var suppressed int
	if err := queryProbeSuppression(ctx, dbPath, "home", &ok, &suppressed); err != nil {
		t.Fatal(err)
	}
	if ok != 0 || suppressed != 1 {
		t.Fatalf("stored probe ok=%d observer_suppressed=%d, want 0 and 1", ok, suppressed)
	}
	incidents, err := store.ListIncidents(ctx, storage.IncidentFilter{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(incidents) != 0 {
		t.Fatalf("incidents = %+v, want none", incidents)
	}
	stats, err := store.ListUptimeStats(ctx, time.Now().UTC(), storage.SingleFailureThreshold(1))
	if err != nil {
		t.Fatal(err)
	}
	if retained := stats["home"].Retained; retained.TotalChecks != 0 || retained.FailedChecks != 0 || retained.DowntimeSeconds != 0 {
		t.Fatalf("retained uptime = %+v, want suppressed probe excluded", retained)
	}
}

func TestMonitorInitialDelayIsDeterministicAndInsideInterval(t *testing.T) {
	interval := 250 * time.Millisecond
	first := monitorInitialDelay("home", interval)
	second := monitorInitialDelay("home", interval)
	if first != second {
		t.Fatalf("delay = %s then %s, want deterministic value", first, second)
	}
	if first <= 0 || first >= interval {
		t.Fatalf("delay = %s, want > 0 and < %s", first, interval)
	}
	other := monitorInitialDelay("api", interval)
	if other <= 0 || other >= interval {
		t.Fatalf("other delay = %s, want > 0 and < %s", other, interval)
	}
}

func TestRunMonitorWaitsForInitialStaggerDelayBeforeFirstProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	interval := 50 * time.Millisecond
	monitorID := monitorIDWithMinimumDelay(t, interval, 20*time.Millisecond)
	mon := config.MonitorConfig{
		ID:                 monitorID,
		Name:               "Home",
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		Interval:           config.Duration{Duration: interval},
	}
	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	store, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}
	runner.emailer = &fakeIncidentSender{}

	go runner.runMonitor(ctx, 1, 0, 0, mon)

	time.Sleep(5 * time.Millisecond)
	if got := atomic.LoadInt32(&requests); got != 0 {
		t.Fatalf("requests before stagger delay = %d, want 0", got)
	}

	deadline := time.After(150 * time.Millisecond)
	for atomic.LoadInt32(&requests) == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for first staggered probe")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestApplyConfigKeepsUnchangedWorker(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}

	cfg := testRunnerConfig("home", "https://example.com/")
	workerConfig := monitorWorkerConfig{
		Monitor:           cfg.Monitors[0],
		FailureThreshold:  cfg.Monitors[0].FailureThreshold,
		ProbeRetries:      cfg.Defaults.ProbeRetries,
		ProbeRetryBackoff: cfg.Defaults.ProbeRetryBackoff.Duration,
	}
	var cancelled int32
	runner.workers["home"] = monitorWorker{
		cancel: func() { atomic.AddInt32(&cancelled, 1) },
		config: workerConfig,
	}

	runner.applyConfig(context.Background(), cfg)

	if got := atomic.LoadInt32(&cancelled); got != 0 {
		t.Fatalf("cancelled unchanged worker %d times, want 0", got)
	}
	if len(runner.workers) != 1 {
		t.Fatalf("worker count = %d, want 1", len(runner.workers))
	}
}

func TestApplyConfigRestartsChangedWorker(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	store, err := storage.Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}

	oldCfg := testRunnerConfig("home", "https://example.com/")
	oldWorkerConfig := monitorWorkerConfig{
		Monitor:           oldCfg.Monitors[0],
		FailureThreshold:  oldCfg.Monitors[0].FailureThreshold,
		ProbeRetries:      oldCfg.Defaults.ProbeRetries,
		ProbeRetryBackoff: oldCfg.Defaults.ProbeRetryBackoff.Duration,
	}
	var cancelled int32
	runner.workers["home"] = monitorWorker{
		cancel: func() { atomic.AddInt32(&cancelled, 1) },
		config: oldWorkerConfig,
	}
	nextCfg := testRunnerConfig("home", "https://changed.example.com/")

	runner.applyConfig(parent, nextCfg)

	if got := atomic.LoadInt32(&cancelled); got != 1 {
		t.Fatalf("cancelled changed worker %d times, want 1", got)
	}
	worker := runner.workers["home"]
	if worker.config.Monitor.URL != "https://changed.example.com/" {
		t.Fatalf("worker URL = %q, want changed URL", worker.config.Monitor.URL)
	}
	cancelParent()
}

func TestApplyConfigRestartsWorkerWhenRelevantDefaultChanges(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	store, err := storage.Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}

	oldCfg := testRunnerConfig("home", "https://example.com/")
	oldWorkerConfig := monitorWorkerConfig{
		Monitor:           oldCfg.Monitors[0],
		FailureThreshold:  oldCfg.Monitors[0].FailureThreshold,
		ProbeRetries:      oldCfg.Defaults.ProbeRetries,
		ProbeRetryBackoff: oldCfg.Defaults.ProbeRetryBackoff.Duration,
	}
	var cancelled int32
	runner.workers["home"] = monitorWorker{
		cancel: func() { atomic.AddInt32(&cancelled, 1) },
		config: oldWorkerConfig,
	}
	nextCfg := testRunnerConfig("home", "https://example.com/")
	nextCfg.Defaults.FailureThreshold = oldCfg.Defaults.FailureThreshold + 1
	nextCfg.Monitors[0].FailureThreshold = nextCfg.Defaults.FailureThreshold

	runner.applyConfig(parent, nextCfg)

	if got := atomic.LoadInt32(&cancelled); got != 1 {
		t.Fatalf("cancelled worker after default change %d times, want 1", got)
	}
	if runner.workers["home"].config.FailureThreshold != nextCfg.Defaults.FailureThreshold {
		t.Fatalf("worker threshold = %d, want %d", runner.workers["home"].config.FailureThreshold, nextCfg.Defaults.FailureThreshold)
	}
	cancelParent()
}

func TestApplyConfigUsesMonitorFailureThresholdOverride(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}

	cfg := testRunnerConfig("home", "https://example.com/")
	cfg.Monitors[0].FailureThreshold = 5

	runner.applyConfig(context.Background(), cfg)

	worker := runner.workers["home"]
	if worker.config.FailureThreshold != 5 {
		t.Fatalf("worker threshold = %d, want monitor override 5", worker.config.FailureThreshold)
	}
}

func TestApplyConfigRestartsWorkerWhenMonitorFailureThresholdChanges(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	store, err := storage.Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}

	oldCfg := testRunnerConfig("home", "https://example.com/")
	oldWorkerConfig := monitorWorkerConfig{
		Monitor:           oldCfg.Monitors[0],
		FailureThreshold:  oldCfg.Monitors[0].FailureThreshold,
		ProbeRetries:      oldCfg.Defaults.ProbeRetries,
		ProbeRetryBackoff: oldCfg.Defaults.ProbeRetryBackoff.Duration,
	}
	var cancelled int32
	runner.workers["home"] = monitorWorker{
		cancel: func() { atomic.AddInt32(&cancelled, 1) },
		config: oldWorkerConfig,
	}
	nextCfg := testRunnerConfig("home", "https://example.com/")
	nextCfg.Monitors[0].FailureThreshold = oldCfg.Monitors[0].FailureThreshold + 2

	runner.applyConfig(parent, nextCfg)

	if got := atomic.LoadInt32(&cancelled); got != 1 {
		t.Fatalf("cancelled worker after monitor threshold change %d times, want 1", got)
	}
	if runner.workers["home"].config.FailureThreshold != nextCfg.Monitors[0].FailureThreshold {
		t.Fatalf("worker threshold = %d, want %d", runner.workers["home"].config.FailureThreshold, nextCfg.Monitors[0].FailureThreshold)
	}
	cancelParent()
}

func TestApplyConfigStopsRemovedWorker(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}

	var cancelled int32
	runner.workers["removed"] = monitorWorker{
		cancel: func() { atomic.AddInt32(&cancelled, 1) },
		config: monitorWorkerConfig{},
	}

	runner.applyConfig(context.Background(), testRunnerConfig("home", "https://example.com/"))

	if got := atomic.LoadInt32(&cancelled); got != 1 {
		t.Fatalf("cancelled removed worker %d times, want 1", got)
	}
	if _, ok := runner.workers["removed"]; ok {
		t.Fatal("removed worker still registered")
	}
}

func TestApplyStatusServerRestartsWhenTenantChanges(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	runner, err := NewRunner("config.yaml", config.Config{}, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("open ephemeral status port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("close ephemeral status port listener: %v", err)
	}
	changedPort := port
	for changedPort == port {
		listenerBeta, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("open second ephemeral status port: %v", err)
		}
		changedPort = listenerBeta.Addr().(*net.TCPAddr).Port
		if err := listenerBeta.Close(); err != nil {
			t.Fatalf("close second ephemeral status port listener: %v", err)
		}
	}

	if err := runner.applyStatusServer(ctx, "127.0.0.1", port, "tenant-alpha"); err != nil {
		t.Fatalf("apply status server first start: %v", err)
	}
	first := runner.statusServer
	if first == nil {
		t.Fatal("status server not started")
	}

	if err := runner.applyStatusServer(ctx, "127.0.0.1", port, "tenant-alpha"); err != nil {
		t.Fatalf("apply status server same tenant: %v", err)
	}
	if runner.statusServer != first {
		t.Fatal("status server restarted for unchanged tenant")
	}

	if err := runner.applyStatusServer(ctx, "127.0.0.1", changedPort, "tenant-beta"); err != nil {
		t.Fatalf("apply status server different tenant: %v", err)
	}
	if runner.statusServer == first {
		t.Fatal("status server not restarted when tenant changed")
	}
	runner.stopStatusServer()
}

func TestApplyStatusServerKeepsReplacementAfterForcedOldServerClose(t *testing.T) {
	checkStarted := make(chan struct{})
	checkTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(checkStarted)
		<-r.Context().Done()
	}))
	defer checkTarget.Close()

	store, err := storage.Open(filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg := config.Config{
		HTTP:     config.HTTPConfig{Auth: config.HTTPAuthConfig{BearerToken: "secret"}},
		TenantID: "default",
		Defaults: config.Defaults{FailureThreshold: 1},
		Monitors: []config.MonitorConfig{{
			ID:                 "slow",
			Name:               "Slow",
			URL:                checkTarget.URL,
			ExpectedStatusCode: http.StatusOK,
			Timeout:            config.Duration{Duration: 30 * time.Second},
		}},
	}
	runner, err := NewRunner("config.yaml", cfg, store, io.Discard, io.Discard, "test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstPort := reserveTCPPort(t)
	secondPort := reserveTCPPort(t)
	for secondPort == firstPort {
		secondPort = reserveTCPPort(t)
	}
	if err := runner.applyStatusServer(ctx, "127.0.0.1", firstPort, "default"); err != nil {
		t.Fatal(err)
	}
	defer runner.stopStatusServer()

	request, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/v1/checks/slow", firstPort), nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer secret")
	requestDone := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if response != nil {
			response.Body.Close()
		}
		requestDone <- err
	}()
	select {
	case <-checkStarted:
	case <-time.After(time.Second):
		t.Fatal("remote check did not start")
	}

	previousTimeout := statusServerShutdownTimeout
	statusServerShutdownTimeout = 20 * time.Millisecond
	t.Cleanup(func() { statusServerShutdownTimeout = previousTimeout })
	if err := runner.applyStatusServer(ctx, "127.0.0.1", secondPort, "default"); err != nil {
		t.Fatalf("replace status server: %v", err)
	}
	response, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", secondPort))
	if err != nil {
		t.Fatalf("replacement server unavailable: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("replacement health status = %d, want 200", response.StatusCode)
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("old remote check was not terminated by forced close")
	}
}

func reserveTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func monitorIDWithMinimumDelay(t *testing.T, interval time.Duration, minimum time.Duration) string {
	t.Helper()
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("monitor-%d", i)
		if monitorInitialDelay(id, interval) >= minimum {
			return id
		}
	}
	t.Fatalf("could not find monitor ID with delay >= %s", minimum)
	return ""
}

func testRunnerConfig(id string, url string) config.Config {
	return config.Config{
		Defaults: config.Defaults{
			Interval:          config.Duration{Duration: time.Minute},
			Timeout:           config.Duration{Duration: time.Second},
			ProbeRetries:      2,
			ProbeRetryBackoff: config.Duration{Duration: time.Millisecond},
			FailureThreshold:  3,
			HistoryRetention:  config.Duration{Duration: 24 * time.Hour},
		},
		Monitors: []config.MonitorConfig{
			{
				ID:                 id,
				Name:               id,
				URL:                url,
				ExpectedStatusCode: http.StatusOK,
				Timeout:            config.Duration{Duration: time.Second},
				Interval:           config.Duration{Duration: time.Minute},
				FailureThreshold:   3,
			},
		},
	}
}

func queryProbeAttempt(ctx context.Context, dbPath string, monitorID string, ok *int, attemptCount *int) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.QueryRowContext(ctx, `SELECT ok, attempt_count FROM probe_results WHERE monitor_id = ?`, monitorID).Scan(ok, attemptCount)
}

func queryProbeSuppression(ctx context.Context, dbPath string, monitorID string, ok *int, suppressed *int) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.QueryRowContext(ctx, `SELECT ok, observer_suppressed FROM probe_results WHERE monitor_id = ?`, monitorID).Scan(ok, suppressed)
}

type fakeIncidentSender struct {
	mu        sync.Mutex
	incidents []storage.Incident
}

func (s *fakeIncidentSender) SendIncident(incident storage.Incident, current storage.MonitorState) []alert.SendResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.incidents = append(s.incidents, incident)
	return []alert.SendResult{{Provider: "smtp"}}
}

func (s *fakeIncidentSender) SendProvider(provider string, incident storage.Incident, current storage.MonitorState) alert.SendResult {
	return alert.SendResult{Provider: provider}
}

func (s *fakeIncidentSender) Providers() []string {
	return []string{"smtp"}
}

func (s *fakeIncidentSender) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.incidents)
}

func (s *fakeIncidentSender) all() []storage.Incident {
	s.mu.Lock()
	defer s.mu.Unlock()
	incidents := make([]storage.Incident, len(s.incidents))
	copy(incidents, s.incidents)
	return incidents
}
