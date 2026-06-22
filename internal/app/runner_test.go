package app

import (
	"context"
	"database/sql"
	"io"
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
	stats, err := store.ListUptimeStats(ctx, time.Now().UTC(), 1)
	if err != nil {
		t.Fatal(err)
	}
	retained := stats["home"].Retained
	if retained.MaintenanceFailedChecks != 1 {
		t.Fatalf("retained stats = %+v, want one failed maintenance check", retained)
	}
	incidents, err := store.ListIncidents(ctx, 10)
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
	incidents, err := store.ListIncidents(ctx, 10)
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

func queryProbeAttempt(ctx context.Context, dbPath string, monitorID string, ok *int, attemptCount *int) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.QueryRowContext(ctx, `SELECT ok, attempt_count FROM probe_results WHERE monitor_id = ?`, monitorID).Scan(ok, attemptCount)
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
