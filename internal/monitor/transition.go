package monitor

import (
	"time"

	"upag/internal/checker"
	"upag/internal/state"
	"upag/internal/storage"
)

type Evaluation struct {
	NextState          storage.MonitorState
	IncidentTransition string
}

func Evaluate(previous storage.MonitorState, result checker.Result, threshold int, now time.Time) Evaluation {
	next := previous
	if next.Status == "" {
		next.Status = state.Unknown
	}
	next.LastCheckedAt = now
	next.LastObservedStatusCode = result.ObservedStatusCode

	if result.OK {
		next.ConsecutiveFailures = 0
		next.LastError = ""
		next.LastSuccessAt = now
		if previous.Status == state.Down {
			next.Status = state.Up
			return Evaluation{NextState: next, IncidentTransition: state.Up}
		}
		next.Status = state.Up
		return Evaluation{NextState: next}
	}

	next.ConsecutiveFailures++
	next.LastFailureAt = now
	next.LastError = checker.FailureMessage(result)
	if previous.Status != state.Down && next.ConsecutiveFailures >= threshold {
		next.Status = state.Down
		return Evaluation{NextState: next, IncidentTransition: state.Down}
	}
	return Evaluation{NextState: next}
}
