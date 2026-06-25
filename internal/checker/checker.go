package checker

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"upag/internal/config"
)

type Result struct {
	OK                 bool
	ObservedStatusCode int
	Latency            time.Duration
	ResponseTime       time.Duration
	Error              string
	CheckedAt          time.Time
}

const maxResponseBodyBytes int64 = 10 << 20

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
	defer client.CloseIdleConnections()

	resp, err := client.Do(req)
	result.Latency = time.Since(start)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer resp.Body.Close()

	result.ObservedStatusCode = resp.StatusCode
	if resp.StatusCode != monitor.ExpectedStatusCode && monitor.MaxResponseTime.Duration == 0 {
		result.Error = fmt.Sprintf("expected HTTP status %d, observed HTTP status %d", monitor.ExpectedStatusCode, resp.StatusCode)
		return result
	}

	var bodyBytes []byte
	var bodyText string
	if monitor.ResponseBody.Configured() || monitor.MaxResponseTime.Duration > 0 {
		body, err := readLimitedResponseBody(resp.Body, maxResponseBodyBytes)
		result.ResponseTime = time.Since(start)
		if err != nil {
			result.Error = fmt.Sprintf("read response body: %v", err)
			return result
		}
		bodyBytes = body
		bodyText = string(body)
	}

	if resp.StatusCode != monitor.ExpectedStatusCode {
		result.Error = fmt.Sprintf("expected HTTP status %d, observed HTTP status %d", monitor.ExpectedStatusCode, resp.StatusCode)
		return result
	}

	if !monitor.ResponseBody.Configured() && monitor.MaxResponseTime.Duration == 0 {
		result.OK = true
		return result
	}

	if monitor.ResponseBody.MustContain != "" && !strings.Contains(bodyText, monitor.ResponseBody.MustContain) {
		result.Error = fmt.Sprintf("response body does not contain required string %q", monitor.ResponseBody.MustContain)
		return result
	}
	if monitor.ResponseBody.MustNotContain != "" && strings.Contains(bodyText, monitor.ResponseBody.MustNotContain) {
		result.Error = fmt.Sprintf("response body contains forbidden string %q", monitor.ResponseBody.MustNotContain)
		return result
	}
	if len(monitor.ResponseBody.Command) != 0 {
		if err := runResponseBodyCommand(ctx, monitor.ResponseBody, bodyBytes); err != nil {
			result.Error = err.Error()
			return result
		}
	}
	if monitor.MaxResponseTime.Duration > 0 && result.ResponseTime > monitor.MaxResponseTime.Duration {
		result.Error = fmt.Sprintf("response time %s exceeded maximum %s", result.ResponseTime, monitor.MaxResponseTime.Duration)
		return result
	}

	result.OK = true
	return result
}

func readLimitedResponseBody(r io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response body exceeds %d bytes", limit)
	}
	return body, nil
}

func runResponseBodyCommand(ctx context.Context, assertion config.ResponseBodyAssertions, body []byte) error {
	cmdCtx, cancel := context.WithTimeout(ctx, assertion.CommandTimeout.Duration)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, assertion.Command[0], assertion.Command[1:]...)
	cmd.Stdin = bytes.NewReader(body)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	stderrText := strings.TrimSpace(stderr.String())
	if errors.Is(cmdCtx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("response body command %q exceeded timeout %s%s", assertion.Command[0], assertion.CommandTimeout.Duration, stderrExcerpt(stderrText))
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("response body command %q failed with exit status %d%s", assertion.Command[0], exitErr.ExitCode(), stderrExcerpt(stderrText))
		}
		return fmt.Errorf("run response body command %q: %v", assertion.Command[0], err)
	}
	return nil
}

func stderrExcerpt(stderr string) string {
	if stderr == "" {
		return ""
	}
	const maxStderrExcerpt = 512
	if len(stderr) > maxStderrExcerpt {
		stderr = stderr[:maxStderrExcerpt] + "..."
	}
	return fmt.Sprintf(": stderr=%q", stderr)
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
