package observer

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"time"

	"upag/internal/config"
	"upag/internal/state"
	"upag/internal/storage"
)

const (
	MonitorID = "__observer__"
	Name      = "Observer Connectivity"
)

type CheckResult struct {
	State              storage.ObserverState
	SentinelResults    []storage.ObserverSentinelResult
	IncidentTransition string
}

func Check(ctx context.Context, cfg config.ObserverConfig, previous storage.ObserverState, previousKnown bool, now time.Time) CheckResult {
	results := make([]storage.ObserverSentinelResult, 0, len(cfg.Sentinels))
	successes := 0
	for _, sentinel := range cfg.Sentinels {
		result := checkSentinel(ctx, cfg.Timeout.Duration, sentinel, now)
		if result.OK {
			successes++
		}
		results = append(results, result)
	}
	healthy := successes >= cfg.RequiredSuccesses
	next, transition := Evaluate(previous, previousKnown, healthy, results, cfg.FailureThreshold, cfg.RecoveryThreshold, now)
	return CheckResult{State: next, SentinelResults: results, IncidentTransition: transition}
}

func Evaluate(previous storage.ObserverState, previousKnown bool, healthy bool, results []storage.ObserverSentinelResult, failureThreshold int, recoveryThreshold int, now time.Time) (storage.ObserverState, string) {
	if failureThreshold <= 0 {
		failureThreshold = 1
	}
	if recoveryThreshold <= 0 {
		recoveryThreshold = 1
	}
	next := previous
	if next.Status == "" {
		next.Status = state.ObserverUp
	}
	next.LastCheckedAt = now
	next.UpdatedAt = now
	if healthy {
		next.ConsecutiveFailures = 0
		next.ConsecutiveSuccesses++
		next.LastSuccessAt = now
		next.LastError = ""
		if next.Status == state.ObserverDown && next.ConsecutiveSuccesses >= recoveryThreshold {
			next.Status = state.ObserverUp
			return next, state.ObserverUp
		}
		if !previousKnown && next.ConsecutiveSuccesses >= recoveryThreshold {
			next.Status = state.ObserverUp
		}
		return next, ""
	}

	next.ConsecutiveSuccesses = 0
	next.ConsecutiveFailures++
	next.LastFailureAt = now
	next.LastError = observerError(results)
	if next.Status != state.ObserverDown && next.ConsecutiveFailures >= failureThreshold {
		next.Status = state.ObserverDown
		return next, state.ObserverDown
	}
	return next, ""
}

func checkSentinel(ctx context.Context, timeout time.Duration, sentinel config.SentinelConfig, now time.Time) storage.ObserverSentinelResult {
	start := time.Now().UTC()
	result := storage.ObserverSentinelResult{
		SentinelID:         sentinel.ID,
		Name:               sentinel.Name,
		URL:                sentinel.URL,
		ExpectedStatusCode: sentinel.ExpectedStatusCode,
		CheckedAt:          now,
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, sentinel.URL, nil)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	req.Header.Set("User-Agent", "upag/0.1 observer")
	client := &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		},
	}
	resp, err := client.Do(req)
	result.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer resp.Body.Close()
	result.ObservedStatusCode = resp.StatusCode
	if resp.StatusCode != sentinel.ExpectedStatusCode {
		result.Error = fmt.Sprintf("expected HTTP status %d, observed HTTP status %d", sentinel.ExpectedStatusCode, resp.StatusCode)
		return result
	}
	result.OK = true
	return result
}

func observerError(results []storage.ObserverSentinelResult) string {
	var parts []string
	for _, result := range results {
		if result.OK {
			continue
		}
		if result.Error != "" {
			parts = append(parts, fmt.Sprintf("%s: %s", result.SentinelID, result.Error))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s: unhealthy", result.SentinelID))
	}
	if len(parts) == 0 {
		return "observer connectivity check did not reach required sentinel quorum"
	}
	return strings.Join(parts, "; ")
}
