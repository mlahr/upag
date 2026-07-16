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
	"regexp"
	"strings"
	"time"

	"upag/internal/config"
)

type Result struct {
	OK                 bool
	ObservedStatusCode int
	FinalURL           string
	RedirectsFollowed  int
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
	redirectsFollowed := 0
	maxRedirects := monitor.MaxRedirects
	if maxRedirects == 0 {
		maxRedirects = config.DefaultMaxRedirects
	}

	client := &http.Client{
		Timeout: monitor.Timeout.Duration,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if !monitor.FollowRedirects {
				return http.ErrUseLastResponse
			}
			if len(via) > maxRedirects {
				return fmt.Errorf("stopped after following %d redirects", maxRedirects)
			}
			redirectsFollowed++
			return nil
		},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: monitor.InsecureSkipVerify},
		},
	}
	defer client.CloseIdleConnections()

	resp, err := client.Do(req)
	result.Latency = time.Since(start)
	result.RedirectsFollowed = redirectsFollowed
	recordFinalResponse(&result, resp)
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
	if err := assertRedirectTarget(monitor.RedirectTarget, redirectsFollowed, resp); err != nil {
		result.Error = err.Error()
		return result
	}

	if !monitor.ResponseBody.Configured() && monitor.MaxResponseTime.Duration == 0 {
		result.OK = true
		return result
	}

	for _, s := range monitor.ResponseBody.MustContain {
		if !strings.Contains(bodyText, s) {
			result.Error = fmt.Sprintf("response body does not contain required string %q", s)
			return result
		}
	}
	for _, s := range monitor.ResponseBody.MustNotContain {
		if strings.Contains(bodyText, s) {
			result.Error = fmt.Sprintf("response body contains forbidden string %q", s)
			return result
		}
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

func recordFinalResponse(result *Result, resp *http.Response) {
	if resp == nil {
		return
	}
	result.ObservedStatusCode = resp.StatusCode
	if resp.Request != nil && resp.Request.URL != nil {
		result.FinalURL = resp.Request.URL.String()
	}
}

func assertRedirectTarget(assertion config.RedirectTargetAssertions, redirectsFollowed int, resp *http.Response) error {
	if !assertion.Configured() {
		return nil
	}
	if redirectsFollowed == 0 {
		return errors.New("redirect target assertion requires at least one followed redirect")
	}
	if resp.Request == nil || resp.Request.URL == nil {
		return errors.New("final redirect target URL is unavailable")
	}

	actual := resp.Request.URL.String()
	if assertion.Exact != "" && actual != assertion.Exact {
		return fmt.Errorf("final redirect target URL %q did not exactly match %q", actual, assertion.Exact)
	}
	if assertion.Regex != "" {
		pattern, err := regexp.Compile(`\A(?:` + assertion.Regex + `)\z`)
		if err != nil {
			return fmt.Errorf("invalid redirect target regular expression %q: %v", assertion.Regex, err)
		}
		if !pattern.MatchString(actual) {
			return fmt.Errorf("final redirect target URL %q did not fully match regular expression %q", actual, assertion.Regex)
		}
	}
	return nil
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
