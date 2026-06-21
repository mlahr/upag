package checker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"time"

	"upag/internal/config"
)

type Result struct {
	OK                 bool
	ObservedStatusCode int
	Latency            time.Duration
	Error              string
	CheckedAt          time.Time
}

func Check(ctx context.Context, monitor config.MonitorConfig) Result {
	start := time.Now().UTC()
	result := Result{CheckedAt: start}

	reqCtx, cancel := context.WithTimeout(ctx, monitor.Timeout.Duration)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, monitor.URL, nil)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	req.Header.Set("User-Agent", "upag/0.1")

	client := &http.Client{
		Timeout: monitor.Timeout.Duration,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: monitor.InsecureSkipVerify},
		},
	}

	resp, err := client.Do(req)
	result.Latency = time.Since(start)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer resp.Body.Close()

	result.ObservedStatusCode = resp.StatusCode
	if resp.StatusCode == monitor.ExpectedStatusCode {
		result.OK = true
		return result
	}

	result.Error = fmt.Sprintf("expected HTTP status %d, observed HTTP status %d", monitor.ExpectedStatusCode, resp.StatusCode)
	return result
}

func FailureMessage(result Result) string {
	if result.Error != "" {
		return result.Error
	}
	if result.ObservedStatusCode != 0 {
		return fmt.Sprintf("unexpected HTTP status %d", result.ObservedStatusCode)
	}
	return "unknown failure"
}

func IsContextCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
