package controlapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"upag/internal/config"
	"upag/internal/storage"
)

type fakeStore struct {
	states       []storage.MonitorState
	uptimeStarts map[string]storage.UptimeStreakStarts
	maintenance  []storage.MaintenanceWindow
	addErr       error
	addCalls     int
}

func (s *fakeStore) ListStates(context.Context) ([]storage.MonitorState, error) { return s.states, nil }
func (s *fakeStore) ListUptimeStreakStarts(context.Context) (map[string]storage.UptimeStreakStarts, error) {
	return s.uptimeStarts, nil
}
func (s *fakeStore) EnsureStatusIntervalsBackfilled(context.Context, storage.FailureThresholds) error {
	return nil
}
func (s *fakeStore) ListStatusIntervals(context.Context, storage.StatusIntervalFilter) ([]storage.StatusInterval, error) {
	return nil, nil
}
func (s *fakeStore) ListIncidents(context.Context, storage.IncidentFilter) ([]storage.Incident, error) {
	return nil, nil
}
func (s *fakeStore) ListFailedProbeResults(context.Context, storage.ProbeResultFilter) ([]storage.ProbeResult, error) {
	return nil, nil
}
func (s *fakeStore) GetObserverState(context.Context) (storage.ObserverState, bool, error) {
	return storage.ObserverState{}, false, nil
}
func (s *fakeStore) ListObserverSentinelEvents(context.Context, storage.ObserverSentinelEventFilter) ([]storage.ObserverSentinelResult, error) {
	return nil, nil
}
func (s *fakeStore) ListMaintenanceWindows(context.Context, storage.MaintenanceWindowFilter) ([]storage.MaintenanceWindow, error) {
	return s.maintenance, nil
}
func (s *fakeStore) AddMaintenanceWindow(context.Context, storage.MaintenanceWindow) (int64, error) {
	s.addCalls++
	if s.addErr != nil {
		return 0, s.addErr
	}
	return 7, nil
}

func TestMaintenanceRejectsOversizedBodyBeforeMutation(t *testing.T) {
	store := &fakeStore{}
	handler := NewHandler(store, func() Runtime { return Runtime{BearerToken: "secret"} })
	valid := `{"monitor_id":"edge","starts_at":"2026-07-21T01:00:00Z","ends_at":"2026-07-21T02:00:00Z","reason":"deploy","created_by":"operator"}`
	body := valid + strings.Repeat(" ", maxRequestBody+1-len(valid)) + `{}`
	request := httptest.NewRequest(http.MethodPost, "/v1/maintenance", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer secret")
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413: %s", recorder.Code, recorder.Body.String())
	}
	if store.addCalls != 0 {
		t.Fatalf("AddMaintenanceWindow calls = %d, want 0", store.addCalls)
	}
}
func (s *fakeStore) CancelMaintenanceWindow(context.Context, int64, time.Time, string, string) error {
	return nil
}

func TestV1RequiresConfiguredBearerToken(t *testing.T) {
	runtime := Runtime{Version: "test", StartedAt: time.Date(2026, 7, 21, 1, 2, 3, 0, time.UTC)}
	handler := NewHandler(&fakeStore{}, func() Runtime { return runtime })

	request := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("without token status = %d, want 404", recorder.Code)
	}

	runtime.BearerToken = "secret"
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/status", nil))
	if recorder.Code != http.StatusUnauthorized || recorder.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("unauthenticated response = %d, WWW-Authenticate %q", recorder.Code, recorder.Header().Get("WWW-Authenticate"))
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	request.Header.Set("Authorization", "Bearer secret")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("authenticated status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var response StatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Version != "test" || response.Status != "ok" {
		t.Fatalf("response = %+v", response)
	}
}

func TestUptimeResponseFromStorage(t *testing.T) {
	generatedAt := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	response := UptimeResponseFromStorage([]storage.MonitorState{
		{MonitorID: "home", Name: "Homepage", Status: "UP"},
		{MonitorID: "never", Name: "Never Failed", Status: "UP"},
		{MonitorID: "future", Name: "Future", Status: "UP"},
	}, map[string]storage.UptimeStreakStarts{
		"home":   {FailureFreeSince: generatedAt.Add(-90 * time.Minute), DowntimeFreeSince: generatedAt.Add(-25 * time.Hour)},
		"future": {FailureFreeSince: generatedAt.Add(time.Minute), DowntimeFreeSince: generatedAt.Add(time.Minute)},
	}, generatedAt)

	if !response.GeneratedAt.Equal(generatedAt) || response.Semantics != uptimeSemantics || len(response.Monitors) != 3 {
		t.Fatalf("response = %+v", response)
	}
	home := response.Monitors[0]
	if home.FailureFreeSeconds == nil || *home.FailureFreeSeconds != 5400 {
		t.Fatalf("home failure-free seconds = %#v, want 5400", home.FailureFreeSeconds)
	}
	if home.DowntimeFreeSeconds == nil || *home.DowntimeFreeSeconds != 90000 {
		t.Fatalf("home downtime-free seconds = %#v, want 90000", home.DowntimeFreeSeconds)
	}
	if response.Monitors[1].FailureFreeSince != nil || response.Monitors[1].FailureFreeSeconds != nil || response.Monitors[1].DowntimeFreeSince != nil || response.Monitors[1].DowntimeFreeSeconds != nil {
		t.Fatalf("never-failed monitor = %+v, want null event fields", response.Monitors[1])
	}
	if response.Monitors[2].FailureFreeSince == nil || response.Monitors[2].FailureFreeSeconds != nil || response.Monitors[2].DowntimeFreeSince == nil || response.Monitors[2].DowntimeFreeSeconds != nil {
		t.Fatalf("future monitor = %+v, want timestamps with null ages", response.Monitors[2])
	}
}

func TestUptimeEndpointAndClient(t *testing.T) {
	failureFreeSince := time.Now().UTC().Add(-time.Hour)
	downtimeFreeSince := failureFreeSince.Add(-time.Hour)
	store := &fakeStore{
		states:       []storage.MonitorState{{MonitorID: "home", Name: "Homepage", Status: "UP"}},
		uptimeStarts: map[string]storage.UptimeStreakStarts{"home": {FailureFreeSince: failureFreeSince, DowntimeFreeSince: downtimeFreeSince}},
	}
	server := httptest.NewServer(NewHandler(store, func() Runtime { return Runtime{BearerToken: "secret", TenantID: "tenant-a"} }))
	defer server.Close()
	client, err := NewClient(server.URL, "secret", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Uptime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Monitors) != 1 || response.Monitors[0].MonitorID != "home" || response.Monitors[0].DowntimeFreeSince == nil || !response.Monitors[0].DowntimeFreeSince.Equal(downtimeFreeSince) {
		t.Fatalf("response = %+v", response)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/uptime", nil)
	request.Header.Set("Authorization", "Bearer secret")
	recorder := httptest.NewRecorder()
	NewHandler(store, func() Runtime { return Runtime{BearerToken: "secret"} }).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /v1/uptime status = %d, want 405", recorder.Code)
	}
}

func TestUptimeClientRejectsPreRecoverySchema(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"generated_at": time.Now().UTC(),
			"monitors":     []any{},
		})
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "secret", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Uptime(context.Background()); err == nil || !strings.Contains(err.Error(), "upgrade and restart") {
		t.Fatalf("Uptime error = %v, want incompatible-schema error", err)
	}
}

func TestRemoteCheckUsesConfiguredMonitorAndDoesNotAcceptRequestConfiguration(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer target.Close()
	runtime := Runtime{BearerToken: "secret", Monitors: []config.MonitorConfig{{ID: "edge", Name: "Edge", URL: target.URL, ExpectedStatusCode: http.StatusNoContent, Timeout: config.Duration{Duration: time.Second}}}}
	server := httptest.NewServer(NewHandler(&fakeStore{}, func() Runtime { return runtime }))
	defer server.Close()
	client, err := NewClient(server.URL, "secret", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Check(context.Background(), "edge")
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK || result.MonitorID != "edge" || result.ObservedStatusCode != http.StatusNoContent {
		t.Fatalf("result = %+v", result)
	}
	_, err = client.Check(context.Background(), "missing")
	remoteErr, ok := err.(*RemoteError)
	if !ok || remoteErr.StatusCode != http.StatusNotFound || remoteErr.Code != "monitor_not_found" {
		t.Fatalf("missing error = %#v", err)
	}
}

func TestRemoteCheckPreservesEscapedMonitorIDs(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer target.Close()
	ids := []string{"space id", "日本語", "percent%id", "group/service", ".", ".."}
	monitors := make([]config.MonitorConfig, 0, len(ids))
	for _, id := range ids {
		monitors = append(monitors, config.MonitorConfig{ID: id, Name: id, URL: target.URL, ExpectedStatusCode: http.StatusNoContent, Timeout: config.Duration{Duration: time.Second}})
	}
	server := httptest.NewServer(NewHandler(&fakeStore{}, func() Runtime {
		return Runtime{BearerToken: "secret", Monitors: monitors}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "secret", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			result, err := client.Check(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			if !result.OK || result.MonitorID != id {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestClientSendsBearerAndSupportsBasePath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/upag/v1/status" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer direct-token" {
			t.Errorf("authorization = %q", got)
		}
		writeJSON(w, 200, StatusResponse{Status: "ok", Version: "v1", StartedAt: time.Now().UTC()})
	}))
	defer server.Close()
	client, err := NewClient(server.URL+"/upag/", "direct-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Status(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceConflictUsesHTTPConflict(t *testing.T) {
	store := &fakeStore{addErr: fmt.Errorf("%w: overlap", storage.ErrMaintenanceConflict)}
	server := httptest.NewServer(NewHandler(store, func() Runtime { return Runtime{BearerToken: "secret"} }))
	defer server.Close()
	client, err := NewClient(server.URL, "secret", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.AddMaintenance(context.Background(), AddMaintenanceRequest{MonitorID: "edge", StartsAt: time.Now(), EndsAt: time.Now().Add(time.Hour), Reason: "deploy", CreatedBy: "operator"})
	remoteErr, ok := err.(*RemoteError)
	if !ok || remoteErr.StatusCode != http.StatusConflict || remoteErr.Code != "conflict" {
		t.Fatalf("error = %#v", err)
	}
}

func TestMaintenanceCreationRejectsRedirectInsteadOfRewritingPost(t *testing.T) {
	redirectedRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirected/v1/maintenance" {
			redirectedRequests++
			writeJSON(w, http.StatusOK, MaintenanceResponse{})
			return
		}
		http.Redirect(w, r, "/redirected/v1/maintenance", http.StatusSeeOther)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "secret", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.AddMaintenance(context.Background(), AddMaintenanceRequest{MonitorID: "edge", StartsAt: time.Now(), EndsAt: time.Now().Add(time.Hour), Reason: "deploy", CreatedBy: "operator"})
	remoteErr, ok := err.(*RemoteError)
	if !ok || remoteErr.StatusCode != http.StatusSeeOther {
		t.Fatalf("error = %#v", err)
	}
	if redirectedRequests != 0 {
		t.Fatalf("redirect target requests = %d, want 0", redirectedRequests)
	}
}

func TestMaintenanceCreationRequiresCreatedStatusAndValidIdentity(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   AddMaintenanceResponse
	}{
		{name: "wrong success status", status: http.StatusOK, body: AddMaintenanceResponse{ID: 9, MonitorID: "edge"}},
		{name: "zero id", status: http.StatusCreated, body: AddMaintenanceResponse{MonitorID: "edge"}},
		{name: "wrong monitor", status: http.StatusCreated, body: AddMaintenanceResponse{ID: 9, MonitorID: "other"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, test.status, test.body) }))
			defer server.Close()
			client, err := NewClient(server.URL, "secret", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.AddMaintenance(context.Background(), AddMaintenanceRequest{MonitorID: "edge"}); err == nil {
				t.Fatal("AddMaintenance succeeded")
			}
		})
	}
}

func TestMaintenanceCancellationRequiresMatchingID(t *testing.T) {
	tests := []struct {
		name string
		body CancelMaintenanceResponse
		ok   bool
	}{
		{name: "empty response", body: CancelMaintenanceResponse{}},
		{name: "wrong id", body: CancelMaintenanceResponse{ID: 8}},
		{name: "matching id", body: CancelMaintenanceResponse{ID: 7}, ok: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(w, http.StatusOK, test.body)
			}))
			defer server.Close()
			client, err := NewClient(server.URL, "secret", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.CancelMaintenance(context.Background(), 7, CancelMaintenanceRequest{CancelledBy: "operator"})
			if test.ok && err != nil {
				t.Fatal(err)
			}
			if !test.ok && err == nil {
				t.Fatal("CancelMaintenance succeeded")
			}
		})
	}
}

func TestParseRemoteURLRejectsCredentialsAndUnsupportedSchemes(t *testing.T) {
	for _, raw := range []string{"ftp://host", "https://user:pass@host", "https://host?token=secret", "host"} {
		if _, err := ParseRemoteURL(raw); err == nil {
			t.Errorf("ParseRemoteURL(%q) succeeded", raw)
		}
	}
}
