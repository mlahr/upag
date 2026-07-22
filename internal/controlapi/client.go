package controlapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxResponseBody = 16 << 20

type Client struct {
	baseURL *url.URL
	token   string
	http    *http.Client
}

type RemoteError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *RemoteError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("remote API returned HTTP %d", e.StatusCode)
}

func ParseRemoteURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse remote URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("remote URL scheme must be http or https")
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("remote URL must include a host")
	}
	if parsed.User != nil {
		return nil, fmt.Errorf("remote URL must not include user information")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("remote URL must not include a query or fragment")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return parsed, nil
}

func NewClient(rawURL, token string, timeout time.Duration) (*Client, error) {
	baseURL, err := ParseRemoteURL(rawURL)
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, fmt.Errorf("remote bearer token is required")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("remote timeout must be positive")
	}
	return &Client{
		baseURL: baseURL,
		token:   token,
		http: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (c *Client) BaseURL() string { return c.baseURL.String() }

func (c *Client) Status(ctx context.Context) (StatusResponse, error) {
	var response StatusResponse
	err := c.do(ctx, http.MethodGet, "/v1/status", nil, nil, &response)
	return response, err
}

func (c *Client) Check(ctx context.Context, monitorID string) (Diagnostic, error) {
	var response Diagnostic
	path := "/v1/checks/" + monitorID
	rawPath := "/v1/checks/" + escapeMonitorID(monitorID)
	err := c.doPath(ctx, http.MethodPost, path, rawPath, nil, nil, &response, http.StatusOK)
	return response, err
}

func escapeMonitorID(monitorID string) string {
	switch monitorID {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	default:
		return url.PathEscape(monitorID)
	}
}

func (c *Client) Monitors(ctx context.Context) (MonitorsResponse, error) {
	var response MonitorsResponse
	err := c.do(ctx, http.MethodGet, "/v1/monitors", nil, nil, &response)
	return response, err
}

func (c *Client) Uptime(ctx context.Context) (UptimeResponse, error) {
	var response UptimeResponse
	err := c.do(ctx, http.MethodGet, "/v1/uptime", nil, nil, &response)
	return response, err
}

func (c *Client) Incidents(ctx context.Context, limit int, since time.Time) (IncidentsResponse, error) {
	query := listQuery(limit, since)
	var response IncidentsResponse
	err := c.do(ctx, http.MethodGet, "/v1/incidents", query, nil, &response)
	return response, err
}

func (c *Client) Intervals(ctx context.Context, monitorID string, limit int, since time.Time) (IntervalsResponse, error) {
	query := listQuery(limit, since)
	if monitorID != "" {
		query.Set("monitor", monitorID)
	}
	var response IntervalsResponse
	err := c.do(ctx, http.MethodGet, "/v1/intervals", query, nil, &response)
	return response, err
}

func (c *Client) Failures(ctx context.Context, limit int, since time.Time) (FailuresResponse, error) {
	query := listQuery(limit, since)
	var response FailuresResponse
	err := c.do(ctx, http.MethodGet, "/v1/failures", query, nil, &response)
	return response, err
}

func (c *Client) Maintenance(ctx context.Context, monitorID string, includeAll bool) (MaintenanceResponse, error) {
	query := url.Values{}
	if monitorID != "" {
		query.Set("monitor", monitorID)
	}
	if includeAll {
		query.Set("all", "true")
	}
	var response MaintenanceResponse
	err := c.do(ctx, http.MethodGet, "/v1/maintenance", query, nil, &response)
	return response, err
}

func (c *Client) AddMaintenance(ctx context.Context, request AddMaintenanceRequest) (AddMaintenanceResponse, error) {
	var response AddMaintenanceResponse
	err := c.doPath(ctx, http.MethodPost, "/v1/maintenance", "/v1/maintenance", nil, request, &response, http.StatusCreated)
	if err == nil && (response.ID <= 0 || response.MonitorID != request.MonitorID) {
		return AddMaintenanceResponse{}, fmt.Errorf("remote API returned an invalid maintenance creation response")
	}
	return response, err
}

func (c *Client) CancelMaintenance(ctx context.Context, id int64, request CancelMaintenanceRequest) (CancelMaintenanceResponse, error) {
	if id <= 0 {
		return CancelMaintenanceResponse{}, fmt.Errorf("maintenance window ID must be positive")
	}
	var response CancelMaintenanceResponse
	err := c.do(ctx, http.MethodPost, "/v1/maintenance/"+strconv.FormatInt(id, 10)+"/cancel", nil, request, &response)
	if err == nil && response.ID != id {
		return CancelMaintenanceResponse{}, fmt.Errorf("remote API returned an invalid maintenance cancellation response")
	}
	return response, err
}

func listQuery(limit int, since time.Time) url.Values {
	query := url.Values{}
	query.Set("limit", strconv.Itoa(limit))
	if !since.IsZero() {
		query.Set("since", since.UTC().Format(time.RFC3339Nano))
	}
	return query
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, requestBody any, responseBody any) error {
	return c.doPath(ctx, method, path, path, query, requestBody, responseBody, http.StatusOK)
}

func (c *Client) doPath(ctx context.Context, method, path, rawPath string, query url.Values, requestBody any, responseBody any, expectedStatus int) error {
	target := *c.baseURL
	target.Path = strings.TrimRight(c.baseURL.Path, "/") + path
	target.RawPath = strings.TrimRight(c.baseURL.EscapedPath(), "/") + rawPath
	target.RawQuery = query.Encode()
	var body io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("encode remote request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return fmt.Errorf("create remote request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.token)
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("remote request to %s failed: %w", c.baseURL.Redacted(), err)
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, maxResponseBody+1)
	if response.StatusCode != expectedStatus {
		var envelope apiError
		if err := json.NewDecoder(limited).Decode(&envelope); err != nil {
			return &RemoteError{StatusCode: response.StatusCode, Message: fmt.Sprintf("remote API returned HTTP %d", response.StatusCode)}
		}
		return &RemoteError{StatusCode: response.StatusCode, Code: envelope.Error.Code, Message: envelope.Error.Message}
	}
	if responseBody == nil {
		return nil
	}
	if err := json.NewDecoder(limited).Decode(responseBody); err != nil {
		return fmt.Errorf("decode remote response: %w", err)
	}
	return nil
}
