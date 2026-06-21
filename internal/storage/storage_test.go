package storage

import (
	"context"
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
	err = store.SaveProbeAndState(context.Background(), ProbeResult{
		MonitorID: "home",
		CheckedAt: now,
		OK:        false,
		Error:     "timeout",
	}, next, incident)
	if err != nil {
		t.Fatal(err)
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
