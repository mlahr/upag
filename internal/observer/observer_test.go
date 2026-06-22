package observer

import (
	"testing"
	"time"

	"upag/internal/state"
	"upag/internal/storage"
)

func TestEvaluateTransitionsDownAfterFailureThreshold(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	previous := storage.ObserverState{
		Status:              state.ObserverUp,
		ConsecutiveFailures: 2,
	}

	next, transition := Evaluate(previous, true, false, []storage.ObserverSentinelResult{
		{SentinelID: "one", Error: "timeout"},
	}, 3, 1, now)

	if next.Status != state.ObserverDown {
		t.Fatalf("status = %s, want OBSERVER_DOWN", next.Status)
	}
	if transition != state.ObserverDown {
		t.Fatalf("transition = %q, want OBSERVER_DOWN", transition)
	}
	if next.ConsecutiveFailures != 3 || next.ConsecutiveSuccesses != 0 {
		t.Fatalf("counters = failures:%d successes:%d, want 3 and 0", next.ConsecutiveFailures, next.ConsecutiveSuccesses)
	}
	if next.LastError == "" {
		t.Fatal("last error is empty, want sentinel error detail")
	}
}

func TestEvaluateTransitionsUpAfterRecoveryThreshold(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	previous := storage.ObserverState{
		Status:              state.ObserverDown,
		ConsecutiveFailures: 5,
		LastError:           "timeout",
	}

	next, transition := Evaluate(previous, true, true, nil, 3, 1, now)

	if next.Status != state.ObserverUp {
		t.Fatalf("status = %s, want OBSERVER_UP", next.Status)
	}
	if transition != state.ObserverUp {
		t.Fatalf("transition = %q, want OBSERVER_UP", transition)
	}
	if next.ConsecutiveFailures != 0 || next.ConsecutiveSuccesses != 1 || next.LastError != "" {
		t.Fatalf("state = %+v, want reset failures, one success, and empty error", next)
	}
}

func TestEvaluateDoesNotDuplicateDownTransition(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	previous := storage.ObserverState{
		Status:              state.ObserverDown,
		ConsecutiveFailures: 3,
	}

	next, transition := Evaluate(previous, true, false, nil, 3, 1, now)

	if next.Status != state.ObserverDown {
		t.Fatalf("status = %s, want OBSERVER_DOWN", next.Status)
	}
	if transition != "" {
		t.Fatalf("transition = %q, want none", transition)
	}
}
