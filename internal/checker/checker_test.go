package checker

import (
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"upag/internal/config"
)

func TestCheckRequiresExactStatusCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
	})
	if result.OK {
		t.Fatal("expected check to fail for non-exact status code")
	}
	if result.ObservedStatusCode != http.StatusNoContent {
		t.Fatalf("observed status = %d, want 204", result.ObservedStatusCode)
	}
}

func TestCheckDoesNotFollowRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/target", http.StatusFound)
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusFound,
		Timeout:            config.Duration{Duration: time.Second},
	})
	if !result.OK {
		t.Fatalf("expected redirect response to satisfy exact 302 check, got error %q", result.Error)
	}
}

func TestCheckFollowsRedirectsAndAssertsFinalTargetAndBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			http.Redirect(w, r, "/intermediate", http.StatusFound)
		case "/intermediate":
			http.Redirect(w, r, "/final?source=redirect", http.StatusMovedPermanently)
		case "/final":
			_, _ = w.Write([]byte("final response body"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		FollowRedirects:    true,
		MaxRedirects:       2,
		RedirectTarget: config.RedirectTargetAssertions{
			Exact: server.URL + "/final?source=redirect",
		},
		ResponseBody: config.ResponseBodyAssertions{
			MustContain: []string{"final response"},
		},
	})
	if !result.OK {
		t.Fatalf("expected final redirect target and body assertions to pass, got error %q", result.Error)
	}
	if result.FinalURL != server.URL+"/final?source=redirect" {
		t.Fatalf("final URL = %q, want %q", result.FinalURL, server.URL+"/final?source=redirect")
	}
	if result.RedirectsFollowed != 2 {
		t.Fatalf("redirects followed = %d, want 2", result.RedirectsFollowed)
	}
}

func TestCheckRedirectTargetRegexMustMatchEntireFinalURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/users/42", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	monitor := config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		FollowRedirects:    true,
		MaxRedirects:       1,
		RedirectTarget: config.RedirectTargetAssertions{
			Regex: `/users/[0-9]+`,
		},
	}
	result := Check(context.Background(), monitor)
	if result.OK {
		t.Fatal("expected substring-only redirect target regex to fail")
	}
	if !strings.Contains(result.Error, "did not fully match") {
		t.Fatalf("error = %q, want full-match failure", result.Error)
	}

	monitor.RedirectTarget.Regex = regexp.QuoteMeta(server.URL) + `/users/[0-9]+`
	result = Check(context.Background(), monitor)
	if !result.OK {
		t.Fatalf("expected full redirect target regex to pass, got error %q", result.Error)
	}
}

func TestCheckRedirectTargetRequiresFollowedRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		FollowRedirects:    true,
		MaxRedirects:       1,
		RedirectTarget: config.RedirectTargetAssertions{
			Exact: server.URL,
		},
	})
	if result.OK {
		t.Fatal("expected redirect target assertion to fail when no redirect was followed")
	}
	if result.Error != "redirect target assertion requires at least one followed redirect" {
		t.Fatalf("error = %q, want missing redirect failure", result.Error)
	}
}

func TestCheckEnforcesExactRedirectLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			http.Redirect(w, r, "/one", http.StatusFound)
		case "/one":
			http.Redirect(w, r, "/two", http.StatusTemporaryRedirect)
		case "/two":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	monitor := config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		FollowRedirects:    true,
		MaxRedirects:       2,
	}
	result := Check(context.Background(), monitor)
	if !result.OK {
		t.Fatalf("expected two-hop chain to satisfy limit 2, got error %q", result.Error)
	}

	monitor.MaxRedirects = 1
	result = Check(context.Background(), monitor)
	if result.OK {
		t.Fatal("expected two-hop chain to exceed limit 1")
	}
	if result.ObservedStatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("observed status = %d, want 307 from last received redirect", result.ObservedStatusCode)
	}
	if result.FinalURL != server.URL+"/one" {
		t.Fatalf("final URL = %q, want last requested URL %q", result.FinalURL, server.URL+"/one")
	}
	if result.RedirectsFollowed != 1 {
		t.Fatalf("redirects followed = %d, want 1", result.RedirectsFollowed)
	}
	if !strings.Contains(result.Error, "stopped after following 1 redirects") {
		t.Fatalf("error = %q, want redirect-limit failure", result.Error)
	}
}

func TestCheckSucceedsWhenResponseBodyContainsRequiredString(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Welcome to the homepage"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustContain: []string{"Welcome"},
		},
	})
	if !result.OK {
		t.Fatalf("expected response body assertion to pass, got error %q", result.Error)
	}
}

func TestCheckFailsWhenResponseBodyDoesNotContainRequiredString(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Temporarily unavailable"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustContain: []string{"Welcome"},
		},
	})
	if result.OK {
		t.Fatal("expected response body assertion to fail")
	}
	if !strings.Contains(result.Error, `does not contain required string "Welcome"`) {
		t.Fatalf("error = %q, want required string failure", result.Error)
	}
}

func TestCheckSucceedsWithMultipleRequiredStrings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Welcome to the homepage of Example"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustContain: []string{"Welcome", "Example"},
		},
	})
	if !result.OK {
		t.Fatalf("expected multiple must_contain assertions to pass, got error %q", result.Error)
	}
}

func TestCheckFailsWhenOneOfMultipleRequiredStringsMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Welcome to the homepage"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustContain: []string{"Welcome", "MissingString"},
		},
	})
	if result.OK {
		t.Fatal("expected missing string in multiple must_contain to fail")
	}
	if !strings.Contains(result.Error, `MissingString`) {
		t.Fatalf("error = %q, want failure for missing string", result.Error)
	}
}

func TestCheckSucceedsWhenResponseBodyDoesNotContainForbiddenString(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Welcome to the homepage"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustNotContain: []string{"Maintenance mode"},
		},
	})
	if !result.OK {
		t.Fatalf("expected forbidden response body assertion to pass, got error %q", result.Error)
	}
}

func TestCheckFailsWhenResponseBodyContainsForbiddenString(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Maintenance mode"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustNotContain: []string{"Maintenance mode"},
		},
	})
	if result.OK {
		t.Fatal("expected forbidden response body assertion to fail")
	}
	if !strings.Contains(result.Error, `contains forbidden string "Maintenance mode"`) {
		t.Fatalf("error = %q, want forbidden string failure", result.Error)
	}
}

func TestCheckSucceedsWithMultipleForbiddenStrings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Welcome to the homepage"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustNotContain: []string{"Error", "Maintenance mode"},
		},
	})
	if !result.OK {
		t.Fatalf("expected multiple must_not_contain assertions to pass, got error %q", result.Error)
	}
}

func TestCheckFailsWhenResponseBodyContainsOneOfMultipleForbiddenStrings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Welcome to the homepage, Error occurred"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustNotContain: []string{"Error", "Maintenance mode"},
		},
	})
	if result.OK {
		t.Fatal("expected one of multiple must_not_contain to fail")
	}
	if !strings.Contains(result.Error, `Error`) {
		t.Fatalf("error = %q, want failure for forbidden string", result.Error)
	}
}

func TestCheckStatusMismatchSkipsResponseBodyAssertions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("Welcome"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			MustContain: []string{"Welcome"},
		},
	})
	if result.OK {
		t.Fatal("expected status mismatch to fail")
	}
	if result.Error != "expected HTTP status 200, observed HTTP status 503" {
		t.Fatalf("error = %q, want status mismatch error", result.Error)
	}
}

func TestCheckSucceedsWhenResponseBodyCommandExitsZero(t *testing.T) {
	t.Setenv("UPAG_CHECKER_HELPER_PROCESS", "1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("Welcome to the homepage"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			Command:        helperCommand("contains", "Welcome"),
			CommandTimeout: config.Duration{Duration: time.Second},
		},
	})
	if !result.OK {
		t.Fatalf("expected response body command assertion to pass, got error %q", result.Error)
	}
}

func TestCheckScrubsRemoteClientEnvironmentFromResponseBodyCommand(t *testing.T) {
	t.Setenv("UPAG_CHECKER_HELPER_PROCESS", "1")
	t.Setenv("UPAG_REMOTE", "https://remote.example")
	t.Setenv("UPAG_TOKEN", "secret")
	t.Setenv("UPAG_REMOTE_TIMEOUT", "5s")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ready"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			Command:        helperCommand("remote-env-absent"),
			CommandTimeout: config.Duration{Duration: 5 * time.Second},
		},
	})
	if !result.OK {
		t.Fatalf("expected scrubbed command environment, got error %q", result.Error)
	}
}

func TestCheckPassesExactResponseBodyBytesToCommandStdin(t *testing.T) {
	t.Setenv("UPAG_CHECKER_HELPER_PROCESS", "1")
	body := []byte{0xff, 'o', 'k', 0x00}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			Command:        helperCommand("hex", hex.EncodeToString(body)),
			CommandTimeout: config.Duration{Duration: time.Second},
		},
	})
	if !result.OK {
		t.Fatalf("expected exact stdin bytes to pass, got error %q", result.Error)
	}
}

func TestCheckFailsWhenResponseBodyCommandExitsNonZero(t *testing.T) {
	t.Setenv("UPAG_CHECKER_HELPER_PROCESS", "1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("bad response"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			Command:        helperCommand("fail"),
			CommandTimeout: config.Duration{Duration: time.Second},
		},
	})
	if result.OK {
		t.Fatal("expected response body command assertion to fail")
	}
	if !strings.Contains(result.Error, `response body command`) || !strings.Contains(result.Error, "exit status 7") || !strings.Contains(result.Error, `stderr="helper failure"`) {
		t.Fatalf("error = %q, want command failure with stderr", result.Error)
	}
}

func TestCheckFailsWhenResponseBodyCommandTimesOut(t *testing.T) {
	t.Setenv("UPAG_CHECKER_HELPER_PROCESS", "1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("slow command"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			Command:        helperCommand("sleep", "200ms"),
			CommandTimeout: config.Duration{Duration: 20 * time.Millisecond},
		},
	})
	if result.OK {
		t.Fatal("expected response body command assertion to time out")
	}
	if !strings.Contains(result.Error, "exceeded timeout 20ms") {
		t.Fatalf("error = %q, want command timeout failure", result.Error)
	}
}

func TestCheckStatusMismatchSkipsResponseBodyCommand(t *testing.T) {
	t.Setenv("UPAG_CHECKER_HELPER_PROCESS", "1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("bad response"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		ResponseBody: config.ResponseBodyAssertions{
			Command:        helperCommand("fail"),
			CommandTimeout: config.Duration{Duration: time.Second},
		},
	})
	if result.OK {
		t.Fatal("expected status mismatch to fail")
	}
	if result.Error != "expected HTTP status 200, observed HTTP status 503" {
		t.Fatalf("error = %q, want status mismatch error", result.Error)
	}
}

func TestReadLimitedResponseBodyAllowsExactLimit(t *testing.T) {
	body, err := readLimitedResponseBody(strings.NewReader("abcd"), 4)
	if err != nil {
		t.Fatalf("readLimitedResponseBody returned error: %v", err)
	}
	if string(body) != "abcd" {
		t.Fatalf("body = %q, want abcd", body)
	}
}

func TestReadLimitedResponseBodyRejectsOversizedBody(t *testing.T) {
	_, err := readLimitedResponseBody(strings.NewReader("abcde"), 4)
	if err == nil {
		t.Fatal("expected oversized body error")
	}
	if !strings.Contains(err.Error(), "response body exceeds 4 bytes") {
		t.Fatalf("error = %q, want size limit failure", err.Error())
	}
}

func TestCheckWithoutResponseBodyAssertionsKeepsStatusOnlyBehavior(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("any response body"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
	})
	if !result.OK {
		t.Fatalf("expected status-only check to pass, got error %q", result.Error)
	}
}

func TestCheckSucceedsWhenResponseTimeIsAtOrBelowMaximum(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		MaxResponseTime:    config.Duration{Duration: time.Second},
	})
	if !result.OK {
		t.Fatalf("expected response time assertion to pass, got error %q", result.Error)
	}
	if result.ResponseTime <= 0 {
		t.Fatalf("response time = %s, want positive duration", result.ResponseTime)
	}
}

func helperCommand(args ...string) []string {
	command := []string{os.Args[0], "-test.run=TestResponseBodyCommandHelperProcess", "--"}
	return append(command, args...)
}

func TestResponseBodyCommandHelperProcess(t *testing.T) {
	if os.Getenv("UPAG_CHECKER_HELPER_PROCESS") != "1" {
		return
	}
	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) < 2 {
		os.Exit(2)
	}
	body, err := io.ReadAll(os.Stdin)
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	switch args[1] {
	case "contains":
		if len(args) != 3 || !strings.Contains(string(body), args[2]) {
			os.Exit(1)
		}
	case "hex":
		if len(args) != 3 || hex.EncodeToString(body) != args[2] {
			os.Exit(1)
		}
	case "fail":
		_, _ = os.Stderr.WriteString("helper failure")
		os.Exit(7)
	case "sleep":
		if len(args) != 3 {
			os.Exit(2)
		}
		duration, err := time.ParseDuration(args[2])
		if err != nil {
			os.Exit(2)
		}
		time.Sleep(duration)
	case "remote-env-absent":
		if len(args) != 2 {
			os.Exit(2)
		}
		for _, key := range []string{"UPAG_REMOTE", "UPAG_TOKEN", "UPAG_REMOTE_TIMEOUT"} {
			if _, exists := os.LookupEnv(key); exists {
				os.Exit(1)
			}
		}
	default:
		os.Exit(2)
	}
	os.Exit(0)
}

func TestCheckFailsWhenResponseTimeExceedsMaximum(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte("slow body"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		MaxResponseTime:    config.Duration{Duration: time.Millisecond},
	})
	if result.OK {
		t.Fatal("expected response time assertion to fail")
	}
	if !strings.Contains(result.Error, "response time") || !strings.Contains(result.Error, "exceeded maximum 1ms") {
		t.Fatalf("error = %q, want response time failure", result.Error)
	}
}

func TestCheckRecordsLatencyAndResponseTimeSeparately(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(30 * time.Millisecond)
		_, _ = w.Write([]byte("delayed body"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		MaxResponseTime:    config.Duration{Duration: time.Second},
	})
	if !result.OK {
		t.Fatalf("expected check to pass, got error %q", result.Error)
	}
	if result.Latency <= 0 {
		t.Fatalf("latency = %s, want positive duration", result.Latency)
	}
	if result.ResponseTime <= result.Latency {
		t.Fatalf("response time = %s, want greater than latency %s", result.ResponseTime, result.Latency)
	}
}

func TestCheckStatusMismatchPrecedesResponseTimeFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(20 * time.Millisecond)
		_, _ = w.Write([]byte("slow failure"))
	}))
	defer server.Close()

	result := Check(context.Background(), config.MonitorConfig{
		URL:                server.URL,
		ExpectedStatusCode: http.StatusOK,
		Timeout:            config.Duration{Duration: time.Second},
		MaxResponseTime:    config.Duration{Duration: time.Millisecond},
	})
	if result.OK {
		t.Fatal("expected status mismatch to fail")
	}
	if result.Error != "expected HTTP status 200, observed HTTP status 503" {
		t.Fatalf("error = %q, want status mismatch error", result.Error)
	}
	if result.ResponseTime <= 0 {
		t.Fatalf("response time = %s, want positive duration", result.ResponseTime)
	}
}
