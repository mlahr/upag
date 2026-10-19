package observer

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"upag/internal/config"
	"upag/internal/state"
	"upag/internal/storage"
)

const (
	MonitorID                = "__observer__"
	Name                     = "Observer Connectivity"
	TransferTargetID         = "cloudflare-100kb"
	TransferTargetName       = "Cloudflare 100 KB transfer"
	TransferURL              = "https://speed.cloudflare.com/__down?bytes=100000"
	transferBytes      int64 = 100000
)

type CheckResult struct {
	State              storage.ObserverState
	SentinelResults    []storage.ObserverSentinelResult
	IncidentTransition string
}

func Check(ctx context.Context, cfg config.ObserverConfig, previous storage.ObserverState, previousKnown bool, now time.Time) CheckResult {
	results := make([]storage.ObserverSentinelResult, len(cfg.Sentinels))
	var wg sync.WaitGroup
	for index, sentinel := range cfg.Sentinels {
		wg.Add(1)
		go func(index int, sentinel config.SentinelConfig) {
			defer wg.Done()
			results[index] = checkSentinel(ctx, cfg.Timeout.Duration, sentinel, now)
		}(index, sentinel)
	}
	wg.Wait()
	successes := 0
	for _, result := range results {
		if result.OK {
			successes++
		}
	}
	healthy := successes >= cfg.RequiredSuccesses
	next, transition := Evaluate(previous, previousKnown, healthy, results, cfg.FailureThreshold, cfg.RecoveryThreshold, now)
	return CheckResult{State: next, SentinelResults: results, IncidentTransition: transition}
}

func CheckTransfer(ctx context.Context, cfg config.TransferCheckConfig, source string, monitorID string, now time.Time) storage.ConnectivitySample {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	timeout := cfg.Timeout.Duration
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	started := time.Now()
	sample := storage.ConnectivitySample{
		Kind: "transfer", Source: source, TargetID: TransferTargetID, TargetName: TransferTargetName,
		URL: TransferURL, MonitorID: monitorID, ExpectedStatusCode: http.StatusOK, CheckedAt: now,
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, TransferURL, nil)
	if err != nil {
		sample.Error = err.Error()
		sample.Slow = true
		sample.ResponseTimeMS = time.Since(started).Milliseconds()
		return sample
	}
	req.Header.Set("User-Agent", "upag/0.1 observer-transfer")
	client := &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
		Transport:     &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, DisableCompression: true},
	}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	sample.LatencyMS = time.Since(started).Milliseconds()
	if err != nil {
		sample.Error = err.Error()
		sample.ResponseTimeMS = time.Since(started).Milliseconds()
		sample.Slow = true
		return sample
	}
	defer resp.Body.Close()
	sample.ObservedStatusCode = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		sample.Error = fmt.Sprintf("expected HTTP status %d, observed HTTP status %d", http.StatusOK, resp.StatusCode)
		sample.ResponseTimeMS = time.Since(started).Milliseconds()
		return sample
	}
	bytesRead, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, transferBytes+1))
	sample.BytesRead = bytesRead
	sample.ResponseTimeMS = time.Since(started).Milliseconds()
	if readErr != nil {
		sample.Error = fmt.Sprintf("read transfer response: %v", readErr)
		sample.Slow = true
		return sample
	}
	if bytesRead != transferBytes {
		sample.Error = fmt.Sprintf("expected %d bytes, received %d", transferBytes, bytesRead)
		sample.Slow = true
		return sample
	}
	sample.OK = true
	sample.Slow = time.Since(started) > cfg.SlowThreshold.Duration
	return sample
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
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	result.LatencyMS = time.Since(start).Milliseconds()
	result.ResponseTimeMS = result.LatencyMS
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
