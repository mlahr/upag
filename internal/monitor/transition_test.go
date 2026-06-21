package monitor

import (
	"testing"
	"time"

	"upag/internal/checker"
	"upag/internal/state"
	"upag/internal/storage"
)

func TestEvaluateDownAfterThreshold(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	previous := storage.MonitorState{Status: state.Up, ConsecutiveFailures: 2}
	result := checker.Result{OK: false, Error: "timeout"}

	evaluation := Evaluate(previous, result, 3, now)
	if evaluation.NextState.Status != state.Down {
		t.Fatalf("status = %s, want DOWN", evaluation.NextState.Status)
	}
	if evaluation.IncidentTransition != state.Down {
		t.Fatalf("transition = %q, want DOWN", evaluation.IncidentTransition)
	}
}

func TestEvaluateRecoveryFromDown(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	previous := storage.MonitorState{Status: state.Down, ConsecutiveFailures: 5, LastError: "timeout"}
	result := checker.Result{OK: true, ObservedStatusCode: 200}

	evaluation := Evaluate(previous, result, 3, now)
	if evaluation.NextState.Status != state.Up {
		t.Fatalf("status = %s, want UP", evaluation.NextState.Status)
	}
	if evaluation.NextState.ConsecutiveFailures != 0 {
		t.Fatalf("failures = %d, want 0", evaluation.NextState.ConsecutiveFailures)
	}
	if evaluation.IncidentTransition != state.Up {
		t.Fatalf("transition = %q, want UP", evaluation.IncidentTransition)
	}
}
