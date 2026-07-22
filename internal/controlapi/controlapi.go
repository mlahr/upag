package controlapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"upag/internal/checker"
	"upag/internal/config"
	"upag/internal/storage"
)

const (
	maxRequestBody  = 64 << 10
	uptimeSemantics = "recovery_streaks_v1"
)

type Store interface {
	ListStates(context.Context) ([]storage.MonitorState, error)
	ListUptimeStreakStarts(context.Context) (map[string]storage.UptimeStreakStarts, error)
	EnsureStatusIntervalsBackfilled(context.Context, storage.FailureThresholds) error
	ListStatusIntervals(context.Context, storage.StatusIntervalFilter) ([]storage.StatusInterval, error)
	ListIncidents(context.Context, storage.IncidentFilter) ([]storage.Incident, error)
	ListFailedProbeResults(context.Context, storage.ProbeResultFilter) ([]storage.ProbeResult, error)
	GetObserverState(context.Context) (storage.ObserverState, bool, error)
	ListObserverSentinelEvents(context.Context, storage.ObserverSentinelEventFilter) ([]storage.ObserverSentinelResult, error)
	ListMaintenanceWindows(context.Context, storage.MaintenanceWindowFilter) ([]storage.MaintenanceWindow, error)
	AddMaintenanceWindow(context.Context, storage.MaintenanceWindow) (int64, error)
	CancelMaintenanceWindow(context.Context, int64, time.Time, string, string) error
}

type Runtime struct {
	Version           string
	StartedAt         time.Time
	TenantID          string
	BearerToken       string
	Monitors          []config.MonitorConfig
	FailureThresholds storage.FailureThresholds
}

type RuntimeProvider func() Runtime

type StatusResponse struct {
	Status    string    `json:"status"`
	Version   string    `json:"version"`
	StartedAt time.Time `json:"started_at"`
}

type Diagnostic struct {
	MonitorID          string    `json:"monitor_id"`
	Name               string    `json:"name"`
	ConfiguredURL      string    `json:"configured_url"`
	FinalURL           string    `json:"final_url"`
	OK                 bool      `json:"ok"`
	ExpectedStatusCode int       `json:"expected_status_code"`
	ObservedStatusCode int       `json:"observed_status_code"`
	RedirectsFollowed  int       `json:"redirects_followed"`
	LatencyMS          int64     `json:"latency_ms"`
	ResponseTimeMS     int64     `json:"response_time_ms"`
	CheckedAt          time.Time `json:"checked_at"`
	Error              string    `json:"error"`
}

type Monitor struct {
	ID                      string    `json:"id"`
	Name                    string    `json:"name"`
	URL                     string    `json:"url"`
	ExpectedStatusCode      int       `json:"expected_status_code"`
	Status                  string    `json:"status"`
	StatusBeforeMaintenance string    `json:"status_before_maintenance"`
	ConsecutiveFailures     int       `json:"consecutive_failures"`
	LastCheckedAt           time.Time `json:"last_checked_at"`
	LastSuccessAt           time.Time `json:"last_success_at"`
	LastFailureAt           time.Time `json:"last_failure_at"`
	LastError               string    `json:"last_error"`
	LastObservedStatusCode  int       `json:"last_observed_status_code"`
	UpdatedAt               time.Time `json:"updated_at"`
}

type Maintenance struct {
	ID                 int64     `json:"id"`
	MonitorID          string    `json:"monitor_id"`
	StartsAt           time.Time `json:"starts_at"`
	EndsAt             time.Time `json:"ends_at"`
	Reason             string    `json:"reason"`
	CreatedBy          string    `json:"created_by"`
	CreatedAt          time.Time `json:"created_at"`
	CancelledAt        time.Time `json:"cancelled_at"`
	CancelledBy        string    `json:"cancelled_by"`
	CancellationReason string    `json:"cancellation_reason"`
}

type MonitorsResponse struct {
	GeneratedAt       time.Time     `json:"generated_at"`
	Monitors          []Monitor     `json:"monitors"`
	ActiveMaintenance []Maintenance `json:"active_maintenance"`
}

type UptimeMonitor struct {
	MonitorID           string     `json:"monitor_id"`
	Name                string     `json:"name"`
	Status              string     `json:"status"`
	FailureFreeSince    *time.Time `json:"failure_free_since"`
	FailureFreeSeconds  *int64     `json:"failure_free_seconds"`
	DowntimeFreeSince   *time.Time `json:"downtime_free_since"`
	DowntimeFreeSeconds *int64     `json:"downtime_free_seconds"`
}

type UptimeResponse struct {
	GeneratedAt time.Time       `json:"generated_at"`
	Semantics   string          `json:"semantics"`
	Monitors    []UptimeMonitor `json:"monitors"`
}

type Incident struct {
	ID         int64     `json:"id"`
	MonitorID  string    `json:"monitor_id"`
	Name       string    `json:"name"`
	Transition string    `json:"transition"`
	ObservedAt time.Time `json:"observed_at"`
	Error      string    `json:"error"`
	StatusCode int       `json:"status_code"`
}

type IncidentsResponse struct {
	Incidents []Incident `json:"incidents"`
}

type Interval struct {
	ID        int64     `json:"id"`
	MonitorID string    `json:"monitor_id"`
	Status    string    `json:"status"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Downtime  bool      `json:"downtime"`
}

type IntervalsResponse struct {
	GeneratedAt time.Time  `json:"generated_at"`
	Intervals   []Interval `json:"intervals"`
}

type ProbeFailure struct {
	MonitorID           string    `json:"monitor_id"`
	CheckedAt           time.Time `json:"checked_at"`
	OK                  bool      `json:"ok"`
	ObservedStatusCode  int       `json:"observed_status_code"`
	LatencyMS           int64     `json:"latency_ms"`
	ResponseTimeMS      int64     `json:"response_time_ms"`
	AttemptCount        int       `json:"attempt_count"`
	Error               string    `json:"error"`
	MaintenanceWindowID int64     `json:"maintenance_window_id"`
	ObserverSuppressed  bool      `json:"observer_suppressed"`
}

type ObserverState struct {
	Status               string    `json:"status"`
	ConsecutiveFailures  int       `json:"consecutive_failures"`
	ConsecutiveSuccesses int       `json:"consecutive_successes"`
	LastCheckedAt        time.Time `json:"last_checked_at"`
	LastSuccessAt        time.Time `json:"last_success_at"`
	LastFailureAt        time.Time `json:"last_failure_at"`
	LastError            string    `json:"last_error"`
	UpdatedAt            time.Time `json:"updated_at"`
}

type SentinelEvent struct {
	ID                 string    `json:"id"`
	Name               string    `json:"name"`
	URL                string    `json:"url"`
	ExpectedStatusCode int       `json:"expected_status_code"`
	OK                 bool      `json:"ok"`
	ObservedStatusCode int       `json:"observed_status_code"`
	LatencyMS          int64     `json:"latency_ms"`
	Error              string    `json:"error"`
	CheckedAt          time.Time `json:"checked_at"`
}

type FailuresResponse struct {
	FailedProbes   []ProbeFailure  `json:"failed_probes"`
	Observer       ObserverState   `json:"observer"`
	ObserverKnown  bool            `json:"observer_known"`
	SentinelEvents []SentinelEvent `json:"sentinel_events"`
}

type MaintenanceResponse struct {
	GeneratedAt time.Time     `json:"generated_at"`
	Windows     []Maintenance `json:"windows"`
}

type AddMaintenanceRequest struct {
	MonitorID string    `json:"monitor_id"`
	StartsAt  time.Time `json:"starts_at"`
	EndsAt    time.Time `json:"ends_at"`
	Reason    string    `json:"reason"`
	CreatedBy string    `json:"created_by"`
}

type AddMaintenanceResponse struct {
	ID        int64  `json:"id"`
	MonitorID string `json:"monitor_id"`
}

type CancelMaintenanceRequest struct {
	Reason      string `json:"reason"`
	CancelledBy string `json:"cancelled_by"`
}

type CancelMaintenanceResponse struct {
	ID int64 `json:"id"`
}

type apiError struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewHandler(store Store, runtime RuntimeProvider) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snapshot := runtime()
		if snapshot.BearerToken == "" {
			http.NotFound(w, r)
			return
		}
		if !authenticated(r, snapshot.BearerToken) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		serveAuthenticated(w, r, store, snapshot)
	})
}

func serveAuthenticated(w http.ResponseWriter, r *http.Request, store Store, runtime Runtime) {
	switch {
	case r.URL.Path == "/v1/status":
		if !allowMethod(w, r, http.MethodGet) {
			return
		}
		writeJSON(w, http.StatusOK, StatusResponse{Status: "ok", Version: runtime.Version, StartedAt: runtime.StartedAt.UTC()})
	case r.URL.Path == "/v1/monitors":
		if !allowMethod(w, r, http.MethodGet) {
			return
		}
		serveMonitors(w, r, store, runtime)
	case r.URL.Path == "/v1/uptime":
		if !allowMethod(w, r, http.MethodGet) {
			return
		}
		serveUptime(w, r, store, runtime)
	case r.URL.Path == "/v1/incidents":
		if !allowMethod(w, r, http.MethodGet) {
			return
		}
		serveIncidents(w, r, store, runtime)
	case r.URL.Path == "/v1/intervals":
		if !allowMethod(w, r, http.MethodGet) {
			return
		}
		serveIntervals(w, r, store, runtime)
	case r.URL.Path == "/v1/failures":
		if !allowMethod(w, r, http.MethodGet) {
			return
		}
		serveFailures(w, r, store, runtime)
	case r.URL.Path == "/v1/maintenance":
		if r.Method == http.MethodGet {
			serveMaintenance(w, r, store, runtime)
			return
		}
		if r.Method == http.MethodPost {
			addMaintenance(w, r, store, runtime)
			return
		}
		allowMethod(w, r, http.MethodGet, http.MethodPost)
	case strings.HasPrefix(r.URL.Path, "/v1/checks/"):
		if !allowMethod(w, r, http.MethodPost) {
			return
		}
		serveCheck(w, r, runtime)
	case strings.HasPrefix(r.URL.Path, "/v1/maintenance/") && strings.HasSuffix(r.URL.Path, "/cancel"):
		if !allowMethod(w, r, http.MethodPost) {
			return
		}
		cancelMaintenance(w, r, store, runtime)
	default:
		http.NotFound(w, r)
	}
}

func authenticated(r *http.Request, expected string) bool {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return false
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.Contains(token, " ") {
		return false
	}
	want := sha256.Sum256([]byte(expected))
	got := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(want[:], got[:]) == 1
}

func tenantContext(r *http.Request, runtime Runtime) context.Context {
	return storage.WithTenant(r.Context(), runtime.TenantID)
}

func serveCheck(w http.ResponseWriter, r *http.Request, runtime Runtime) {
	rawID := strings.TrimPrefix(r.URL.EscapedPath(), "/v1/checks/")
	if rawID == "" || strings.Contains(rawID, "/") {
		writeError(w, 404, "not_found", "monitor not found")
		return
	}
	id, err := url.PathUnescape(rawID)
	if err != nil || id == "" {
		writeError(w, 404, "not_found", "monitor not found")
		return
	}
	var monitor config.MonitorConfig
	found := false
	for _, candidate := range runtime.Monitors {
		if candidate.ID == id {
			monitor, found = candidate, true
			break
		}
	}
	if !found {
		writeError(w, 404, "monitor_not_found", fmt.Sprintf("monitor %q is not configured", id))
		return
	}
	result := checker.Check(r.Context(), monitor)
	writeJSON(w, http.StatusOK, Diagnostic{
		MonitorID: monitor.ID, Name: monitor.Name, ConfiguredURL: monitor.URL, FinalURL: result.FinalURL,
		OK: result.OK, ExpectedStatusCode: monitor.ExpectedStatusCode, ObservedStatusCode: result.ObservedStatusCode,
		RedirectsFollowed: result.RedirectsFollowed, LatencyMS: result.Latency.Milliseconds(),
		ResponseTimeMS: result.ResponseTime.Milliseconds(), CheckedAt: result.CheckedAt.UTC(), Error: result.Error,
	})
}

func serveMonitors(w http.ResponseWriter, r *http.Request, store Store, runtime Runtime) {
	ctx := tenantContext(r, runtime)
	states, err := store.ListStates(ctx)
	if err != nil {
		internalError(w, err)
		return
	}
	now := time.Now().UTC()
	windows, err := store.ListMaintenanceWindows(ctx, storage.MaintenanceWindowFilter{Now: now})
	if err != nil {
		internalError(w, err)
		return
	}
	active := make([]storage.MaintenanceWindow, 0)
	for _, window := range windows {
		if window.CancelledAt.IsZero() && !now.Before(window.StartsAt) && now.Before(window.EndsAt) {
			active = append(active, window)
		}
	}
	writeJSON(w, 200, MonitorsResponse{GeneratedAt: now, Monitors: MonitorsFromStorage(states), ActiveMaintenance: MaintenanceFromStorage(active)})
}

func serveUptime(w http.ResponseWriter, r *http.Request, store Store, runtime Runtime) {
	ctx := tenantContext(r, runtime)
	if err := store.EnsureStatusIntervalsBackfilled(ctx, runtime.FailureThresholds); err != nil {
		internalError(w, err)
		return
	}
	states, err := store.ListStates(ctx)
	if err != nil {
		internalError(w, err)
		return
	}
	starts, err := store.ListUptimeStreakStarts(ctx)
	if err != nil {
		internalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, UptimeResponseFromStorage(states, starts, time.Now().UTC()))
}

func serveIncidents(w http.ResponseWriter, r *http.Request, store Store, runtime Runtime) {
	limit, since, ok := listFilter(w, r, 50)
	if !ok {
		return
	}
	rows, err := store.ListIncidents(tenantContext(r, runtime), storage.IncidentFilter{Limit: limit, Since: since})
	if err != nil {
		internalError(w, err)
		return
	}
	writeJSON(w, 200, IncidentsResponse{Incidents: IncidentsFromStorage(rows)})
}

func serveIntervals(w http.ResponseWriter, r *http.Request, store Store, runtime Runtime) {
	limit, since, ok := listFilter(w, r, 50)
	if !ok {
		return
	}
	now := time.Now().UTC()
	ctx := tenantContext(r, runtime)
	if err := store.EnsureStatusIntervalsBackfilled(ctx, runtime.FailureThresholds); err != nil {
		internalError(w, err)
		return
	}
	rows, err := store.ListStatusIntervals(ctx, storage.StatusIntervalFilter{MonitorID: r.URL.Query().Get("monitor"), Limit: limit, Since: since, Now: now})
	if err != nil {
		internalError(w, err)
		return
	}
	writeJSON(w, 200, IntervalsResponse{GeneratedAt: now, Intervals: IntervalsFromStorage(rows)})
}

func serveFailures(w http.ResponseWriter, r *http.Request, store Store, runtime Runtime) {
	limit, since, ok := listFilter(w, r, 50)
	if !ok {
		return
	}
	ctx := tenantContext(r, runtime)
	probes, err := store.ListFailedProbeResults(ctx, storage.ProbeResultFilter{Limit: limit, Since: since})
	if err != nil {
		internalError(w, err)
		return
	}
	observer, known, err := store.GetObserverState(ctx)
	if err != nil {
		internalError(w, err)
		return
	}
	events, err := store.ListObserverSentinelEvents(ctx, storage.ObserverSentinelEventFilter{Limit: limit, Since: since})
	if err != nil {
		internalError(w, err)
		return
	}
	writeJSON(w, 200, FailuresResponse{FailedProbes: ProbeFailuresFromStorage(probes), Observer: ObserverStateFromStorage(observer), ObserverKnown: known, SentinelEvents: SentinelEventsFromStorage(events)})
}

func serveMaintenance(w http.ResponseWriter, r *http.Request, store Store, runtime Runtime) {
	includeAll := false
	if raw := r.URL.Query().Get("all"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			writeError(w, 400, "invalid_query", "all must be a boolean")
			return
		}
		includeAll = value
	}
	now := time.Now().UTC()
	filter := storage.MaintenanceWindowFilter{MonitorID: r.URL.Query().Get("monitor"), IncludeAll: includeAll}
	if !includeAll {
		filter.Now = now
	}
	rows, err := store.ListMaintenanceWindows(tenantContext(r, runtime), filter)
	if err != nil {
		internalError(w, err)
		return
	}
	writeJSON(w, 200, MaintenanceResponse{GeneratedAt: now, Windows: MaintenanceFromStorage(rows)})
}

func addMaintenance(w http.ResponseWriter, r *http.Request, store Store, runtime Runtime) {
	var request AddMaintenanceRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	window := storage.MaintenanceWindow{MonitorID: request.MonitorID, StartsAt: request.StartsAt.UTC(), EndsAt: request.EndsAt.UTC(), Reason: request.Reason, CreatedBy: request.CreatedBy, CreatedAt: time.Now().UTC()}
	id, err := store.AddMaintenanceWindow(tenantContext(r, runtime), window)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, AddMaintenanceResponse{ID: id, MonitorID: request.MonitorID})
}

func cancelMaintenance(w http.ResponseWriter, r *http.Request, store Store, runtime Runtime) {
	raw := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/maintenance/"), "/cancel")
	raw = strings.TrimSuffix(raw, "/")
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		writeError(w, 404, "not_found", "maintenance window not found")
		return
	}
	var request CancelMaintenanceRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if err := store.CancelMaintenanceWindow(tenantContext(r, runtime), id, time.Now().UTC(), request.CancelledBy, request.Reason); err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, 200, CancelMaintenanceResponse{ID: id})
}

func listFilter(w http.ResponseWriter, r *http.Request, defaultLimit int) (int, time.Time, bool) {
	limit := defaultLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			writeError(w, 400, "invalid_query", "limit must be a positive integer")
			return 0, time.Time{}, false
		}
		limit = value
	}
	var since time.Time
	if raw := r.URL.Query().Get("since"); raw != "" {
		value, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeError(w, 400, "invalid_query", "since must be an RFC3339 timestamp")
			return 0, time.Time{}, false
		}
		since = value.UTC()
	}
	return limit, since, true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, 400, "invalid_content_type", "Content-Type must be application/json")
		return false
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		writeError(w, 400, "invalid_json", "could not read request body")
		return false
	}
	if len(body) > maxRequestBody {
		writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", fmt.Sprintf("request body must not exceed %d bytes", maxRequestBody))
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, 400, "invalid_json", "request body must contain one valid JSON object")
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, 400, "invalid_json", "request body must contain one valid JSON object")
		return false
	}
	return true
}

func allowMethod(w http.ResponseWriter, r *http.Request, methods ...string) bool {
	for _, method := range methods {
		if r.Method == method {
			return true
		}
	}
	w.Header().Set("Allow", strings.Join(methods, ", "))
	writeError(w, 405, "method_not_allowed", "method not allowed")
	return false
}

func writeDomainError(w http.ResponseWriter, err error) {
	message := err.Error()
	switch {
	case errors.Is(err, storage.ErrMaintenanceNotFound):
		writeError(w, 404, "not_found", message)
	case errors.Is(err, storage.ErrMaintenanceConflict):
		writeError(w, 409, "conflict", message)
	case errors.Is(err, storage.ErrMaintenanceInvalid):
		writeError(w, 400, "invalid_request", message)
	default:
		internalError(w, err)
	}
}

func internalError(w http.ResponseWriter, _ error) {
	writeError(w, 500, "internal_error", "internal server error")
}
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, apiError{Error: errorDetail{Code: code, Message: message}})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func MonitorsFromStorage(rows []storage.MonitorState) []Monitor {
	out := make([]Monitor, 0, len(rows))
	for _, v := range rows {
		out = append(out, Monitor{ID: v.MonitorID, Name: v.Name, URL: v.URL, ExpectedStatusCode: v.ExpectedStatusCode, Status: v.Status, StatusBeforeMaintenance: v.StatusBeforeMaintenance, ConsecutiveFailures: v.ConsecutiveFailures, LastCheckedAt: v.LastCheckedAt, LastSuccessAt: v.LastSuccessAt, LastFailureAt: v.LastFailureAt, LastError: v.LastError, LastObservedStatusCode: v.LastObservedStatusCode, UpdatedAt: v.UpdatedAt})
	}
	return out
}

func UptimeResponseFromStorage(states []storage.MonitorState, starts map[string]storage.UptimeStreakStarts, generatedAt time.Time) UptimeResponse {
	generatedAt = generatedAt.UTC()
	monitors := make([]UptimeMonitor, 0, len(states))
	for _, monitorState := range states {
		streak := starts[monitorState.MonitorID]
		failureFreeSince := utcTimePtr(streak.FailureFreeSince)
		downtimeFreeSince := utcTimePtr(streak.DowntimeFreeSince)
		monitors = append(monitors, UptimeMonitor{
			MonitorID:           monitorState.MonitorID,
			Name:                monitorState.Name,
			Status:              monitorState.Status,
			FailureFreeSince:    failureFreeSince,
			FailureFreeSeconds:  elapsedSeconds(generatedAt, failureFreeSince),
			DowntimeFreeSince:   downtimeFreeSince,
			DowntimeFreeSeconds: elapsedSeconds(generatedAt, downtimeFreeSince),
		})
	}
	return UptimeResponse{GeneratedAt: generatedAt, Semantics: uptimeSemantics, Monitors: monitors}
}

func utcTimePtr(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	value = value.UTC()
	return &value
}

func elapsedSeconds(now time.Time, event *time.Time) *int64 {
	if event == nil || event.After(now) {
		return nil
	}
	seconds := int64(now.Sub(*event) / time.Second)
	return &seconds
}

func (m UptimeMonitor) Storage() storage.MonitorUptime {
	row := storage.MonitorUptime{MonitorID: m.MonitorID, Name: m.Name, Status: m.Status}
	if m.FailureFreeSince != nil {
		row.FailureFreeSince = m.FailureFreeSince.UTC()
	}
	if m.DowntimeFreeSince != nil {
		row.DowntimeFreeSince = m.DowntimeFreeSince.UTC()
	}
	return row
}
func MaintenanceFromStorage(rows []storage.MaintenanceWindow) []Maintenance {
	out := make([]Maintenance, 0, len(rows))
	for _, v := range rows {
		out = append(out, Maintenance{ID: v.ID, MonitorID: v.MonitorID, StartsAt: v.StartsAt, EndsAt: v.EndsAt, Reason: v.Reason, CreatedBy: v.CreatedBy, CreatedAt: v.CreatedAt, CancelledAt: v.CancelledAt, CancelledBy: v.CancelledBy, CancellationReason: v.CancellationReason})
	}
	return out
}
func IncidentsFromStorage(rows []storage.Incident) []Incident {
	out := make([]Incident, 0, len(rows))
	for _, v := range rows {
		out = append(out, Incident{ID: v.ID, MonitorID: v.MonitorID, Name: v.Name, Transition: v.Transition, ObservedAt: v.ObservedAt, Error: v.Error, StatusCode: v.StatusCode})
	}
	return out
}
func IntervalsFromStorage(rows []storage.StatusInterval) []Interval {
	out := make([]Interval, 0, len(rows))
	for _, v := range rows {
		out = append(out, Interval{ID: v.ID, MonitorID: v.MonitorID, Status: v.Status, StartedAt: v.StartedAt, EndedAt: v.EndedAt, Downtime: v.Downtime})
	}
	return out
}
func ProbeFailuresFromStorage(rows []storage.ProbeResult) []ProbeFailure {
	out := make([]ProbeFailure, 0, len(rows))
	for _, v := range rows {
		out = append(out, ProbeFailure{MonitorID: v.MonitorID, CheckedAt: v.CheckedAt, OK: v.OK, ObservedStatusCode: v.ObservedStatusCode, LatencyMS: v.LatencyMS, ResponseTimeMS: v.ResponseTimeMS, AttemptCount: v.AttemptCount, Error: v.Error, MaintenanceWindowID: v.MaintenanceWindowID, ObserverSuppressed: v.ObserverSuppressed})
	}
	return out
}
func ObserverStateFromStorage(v storage.ObserverState) ObserverState {
	return ObserverState{Status: v.Status, ConsecutiveFailures: v.ConsecutiveFailures, ConsecutiveSuccesses: v.ConsecutiveSuccesses, LastCheckedAt: v.LastCheckedAt, LastSuccessAt: v.LastSuccessAt, LastFailureAt: v.LastFailureAt, LastError: v.LastError, UpdatedAt: v.UpdatedAt}
}
func SentinelEventsFromStorage(rows []storage.ObserverSentinelResult) []SentinelEvent {
	out := make([]SentinelEvent, 0, len(rows))
	for _, v := range rows {
		out = append(out, SentinelEvent{ID: v.SentinelID, Name: v.Name, URL: v.URL, ExpectedStatusCode: v.ExpectedStatusCode, OK: v.OK, ObservedStatusCode: v.ObservedStatusCode, LatencyMS: v.LatencyMS, Error: v.Error, CheckedAt: v.CheckedAt})
	}
	return out
}

func (m Monitor) Storage() storage.MonitorState {
	return storage.MonitorState{MonitorID: m.ID, Name: m.Name, URL: m.URL, ExpectedStatusCode: m.ExpectedStatusCode, Status: m.Status, StatusBeforeMaintenance: m.StatusBeforeMaintenance, ConsecutiveFailures: m.ConsecutiveFailures, LastCheckedAt: m.LastCheckedAt, LastSuccessAt: m.LastSuccessAt, LastFailureAt: m.LastFailureAt, LastError: m.LastError, LastObservedStatusCode: m.LastObservedStatusCode, UpdatedAt: m.UpdatedAt}
}
func (m Maintenance) Storage() storage.MaintenanceWindow {
	return storage.MaintenanceWindow{ID: m.ID, MonitorID: m.MonitorID, StartsAt: m.StartsAt, EndsAt: m.EndsAt, Reason: m.Reason, CreatedBy: m.CreatedBy, CreatedAt: m.CreatedAt, CancelledAt: m.CancelledAt, CancelledBy: m.CancelledBy, CancellationReason: m.CancellationReason}
}
func (v Incident) Storage() storage.Incident {
	return storage.Incident{ID: v.ID, MonitorID: v.MonitorID, Name: v.Name, Transition: v.Transition, ObservedAt: v.ObservedAt, Error: v.Error, StatusCode: v.StatusCode}
}
func (v Interval) Storage() storage.StatusInterval {
	return storage.StatusInterval{ID: v.ID, MonitorID: v.MonitorID, Status: v.Status, StartedAt: v.StartedAt, EndedAt: v.EndedAt, Downtime: v.Downtime}
}
func (v ProbeFailure) Storage() storage.ProbeResult {
	return storage.ProbeResult{MonitorID: v.MonitorID, CheckedAt: v.CheckedAt, OK: v.OK, ObservedStatusCode: v.ObservedStatusCode, LatencyMS: v.LatencyMS, ResponseTimeMS: v.ResponseTimeMS, AttemptCount: v.AttemptCount, Error: v.Error, MaintenanceWindowID: v.MaintenanceWindowID, ObserverSuppressed: v.ObserverSuppressed}
}
func (v ObserverState) Storage() storage.ObserverState {
	return storage.ObserverState{Status: v.Status, ConsecutiveFailures: v.ConsecutiveFailures, ConsecutiveSuccesses: v.ConsecutiveSuccesses, LastCheckedAt: v.LastCheckedAt, LastSuccessAt: v.LastSuccessAt, LastFailureAt: v.LastFailureAt, LastError: v.LastError, UpdatedAt: v.UpdatedAt}
}
func (v SentinelEvent) Storage() storage.ObserverSentinelResult {
	return storage.ObserverSentinelResult{SentinelID: v.ID, Name: v.Name, URL: v.URL, ExpectedStatusCode: v.ExpectedStatusCode, OK: v.OK, ObservedStatusCode: v.ObservedStatusCode, LatencyMS: v.LatencyMS, Error: v.Error, CheckedAt: v.CheckedAt}
}
