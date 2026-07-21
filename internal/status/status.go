package status

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"upag/internal/storage"
)

const (
	alertFailureLimit = 50
	dailyHistoryDays  = 90
)

type Store interface {
	ListStates(ctx context.Context) ([]storage.MonitorState, error)
	ListUptimeStats(ctx context.Context, now time.Time, thresholds storage.FailureThresholds) (map[string]storage.UptimeStats, error)
	ListDailyUptimeStats(ctx context.Context, now time.Time, days int, thresholds storage.FailureThresholds) (map[string][]storage.DailyUptimeStats, error)
	ListActionableAlertDeliveryFailures(ctx context.Context, limit int) ([]storage.AlertNotification, error)
	ListMaintenanceWindows(ctx context.Context, filter storage.MaintenanceWindowFilter) ([]storage.MaintenanceWindow, error)
	GetObserverState(ctx context.Context) (storage.ObserverState, bool, error)
	ListObserverSentinelResults(ctx context.Context) ([]storage.ObserverSentinelResult, error)
}

type Metadata struct {
	Version           string
	StartedAt         time.Time
	ConfigPath        string
	MonitorCount      int
	FailureThresholds storage.FailureThresholds
}

type MetadataProvider func() Metadata

type Server struct {
	server       *http.Server
	cancel       context.CancelFunc
	shutdownOnce sync.Once
	shutdownErr  error
}

func Start(ctx context.Context, address string, port int, store Store, tenantID string, metadata MetadataProvider, controlHandlers ...http.Handler) (*Server, error) {
	listenAddress := ListenAddress(address, port)
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", listenAddress, err)
	}

	server := &http.Server{
		Handler:           NewHandler(store, tenantID, metadata, controlHandlers...),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	serverCtx, cancel := context.WithCancel(ctx)
	statusServer := &Server{
		server: server,
		cancel: cancel,
	}
	go func() {
		<-serverCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = statusServer.Shutdown(shutdownCtx)
	}()
	go func() {
		_ = server.Serve(listener)
	}()
	return statusServer, nil
}

func ListenAddress(address string, port int) string {
	return net.JoinHostPort(address, strconv.Itoa(port))
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		s.cancel()
		s.shutdownErr = s.server.Shutdown(ctx)
	})
	return s.shutdownErr
}

func (s *Server) Close() error {
	s.cancel()
	return s.server.Close()
}

func NewHandler(store Store, tenantID string, metadata MetadataProvider, controlHandlers ...http.Handler) http.Handler {
	mux := http.NewServeMux()
	if len(controlHandlers) > 0 && controlHandlers[0] != nil {
		mux.Handle("/v1/", controlHandlers[0])
	}
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if !allowGet(w, r) {
			return
		}
		meta := metadata()
		writeJSON(w, http.StatusOK, healthResponse{
			Status:    "ok",
			Version:   meta.Version,
			StartedAt: timePtr(meta.StartedAt),
		})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if !allowGet(w, r) {
			return
		}
		ctx := r.Context()
		ctx = storage.WithTenant(ctx, tenantID)
		states, err := store.ListStates(ctx)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
		now := time.Now().UTC()
		meta := metadata()
		uptimeStats, err := store.ListUptimeStats(ctx, now, meta.FailureThresholds)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
		maintenance, err := store.ListMaintenanceWindows(ctx, storage.MaintenanceWindowFilter{Now: now})
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
		failures, err := store.ListActionableAlertDeliveryFailures(ctx, alertFailureLimit)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
		observerState, observerKnown, err := store.GetObserverState(ctx)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
		observerSentinels, err := store.ListObserverSentinelResults(ctx)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, statusResponse{
			Status:                "ok",
			Version:               meta.Version,
			StartedAt:             timePtr(meta.StartedAt),
			ConfigPath:            meta.ConfigPath,
			MonitorCount:          meta.MonitorCount,
			Observer:              observerResponseFromState(observerState, observerKnown, observerSentinels),
			Monitors:              monitorResponses(states, uptimeStats, maintenance, now),
			AlertDeliveryFailures: alertFailureResponses(failures),
		})
	})
	mux.HandleFunc("/status/history", func(w http.ResponseWriter, r *http.Request) {
		if !allowGet(w, r) {
			return
		}
		ctx := storage.WithTenant(r.Context(), tenantID)
		states, err := store.ListStates(ctx)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
		now := time.Now().UTC()
		stats, err := store.ListDailyUptimeStats(ctx, now, dailyHistoryDays, metadata().FailureThresholds)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, dailyHistoryResponseFromStats(states, stats, now))
	})
	return mux
}

type healthResponse struct {
	Status    string     `json:"status"`
	Version   string     `json:"version"`
	StartedAt *time.Time `json:"started_at"`
}

type statusResponse struct {
	Status                string                 `json:"status"`
	Version               string                 `json:"version"`
	StartedAt             *time.Time             `json:"started_at"`
	ConfigPath            string                 `json:"config_path"`
	MonitorCount          int                    `json:"monitor_count"`
	Observer              observerResponse       `json:"observer"`
	Monitors              []monitorResponse      `json:"monitors"`
	AlertDeliveryFailures []alertFailureResponse `json:"alert_delivery_failures"`
}

type observerResponse struct {
	Status               string                     `json:"status"`
	ConsecutiveFailures  int                        `json:"consecutive_failures"`
	ConsecutiveSuccesses int                        `json:"consecutive_successes"`
	LastCheckedAt        *time.Time                 `json:"last_checked_at"`
	LastSuccessAt        *time.Time                 `json:"last_success_at"`
	LastFailureAt        *time.Time                 `json:"last_failure_at"`
	LastError            string                     `json:"last_error"`
	UpdatedAt            *time.Time                 `json:"updated_at"`
	Sentinels            []observerSentinelResponse `json:"sentinels"`
}

type observerSentinelResponse struct {
	ID                 string     `json:"id"`
	Name               string     `json:"name"`
	URL                string     `json:"url"`
	OK                 bool       `json:"ok"`
	ExpectedStatusCode int        `json:"expected_status_code"`
	ObservedStatusCode int        `json:"observed_status_code"`
	LatencyMS          int64      `json:"latency_ms"`
	Error              string     `json:"error"`
	CheckedAt          *time.Time `json:"checked_at"`
}

type monitorResponse struct {
	ID                     string                `json:"id"`
	Name                   string                `json:"name"`
	URL                    string                `json:"url"`
	Status                 string                `json:"status"`
	ConsecutiveFailures    int                   `json:"consecutive_failures"`
	LastCheckedAt          *time.Time            `json:"last_checked_at"`
	LastSuccessAt          *time.Time            `json:"last_success_at"`
	LastFailureAt          *time.Time            `json:"last_failure_at"`
	LastError              string                `json:"last_error"`
	LastObservedStatusCode int                   `json:"last_observed_status_code"`
	UpdatedAt              *time.Time            `json:"updated_at"`
	Uptime                 uptimeResponse        `json:"uptime"`
	ActiveMaintenance      *maintenanceResponse  `json:"active_maintenance"`
	UpcomingMaintenance    []maintenanceResponse `json:"upcoming_maintenance"`
}

type uptimeResponse struct {
	TwentyFourHour uptimeWindowResponse `json:"24h"`
	SevenDay       uptimeWindowResponse `json:"7d"`
	ThirtyDay      uptimeWindowResponse `json:"30d"`
	Retained       uptimeWindowResponse `json:"retained"`
}

type uptimeWindowResponse struct {
	TotalChecks             int        `json:"total_checks"`
	SuccessfulChecks        int        `json:"successful_checks"`
	FailedChecks            int        `json:"failed_checks"`
	MaintenanceChecks       int        `json:"maintenance_checks"`
	MaintenanceFailedChecks int        `json:"maintenance_failed_checks"`
	DowntimeSeconds         int64      `json:"downtime_seconds"`
	ReportableSeconds       int64      `json:"reportable_seconds"`
	UptimePercent           *float64   `json:"uptime_percent"`
	WindowStartedAt         *time.Time `json:"window_started_at"`
	WindowEndedAt           *time.Time `json:"window_ended_at"`
}

type dailyHistoryResponse struct {
	Status      string                        `json:"status"`
	GeneratedAt *time.Time                    `json:"generated_at"`
	Timezone    string                        `json:"timezone"`
	Range       dailyHistoryRangeResponse     `json:"range"`
	Monitors    []monitorDailyHistoryResponse `json:"monitors"`
}

type dailyHistoryRangeResponse struct {
	Days      int    `json:"days"`
	StartedOn string `json:"started_on"`
	EndedOn   string `json:"ended_on"`
}

type monitorDailyHistoryResponse struct {
	ID   string                `json:"id"`
	Name string                `json:"name"`
	Days []dailyUptimeResponse `json:"days"`
}

type dailyUptimeResponse struct {
	Date              string   `json:"date"`
	ReportableSeconds int64    `json:"reportable_seconds"`
	DowntimeSeconds   int64    `json:"downtime_seconds"`
	UptimePercent     *float64 `json:"uptime_percent"`
}

type maintenanceResponse struct {
	ID        int64      `json:"id"`
	StartsAt  *time.Time `json:"starts_at"`
	EndsAt    *time.Time `json:"ends_at"`
	Reason    string     `json:"reason"`
	CreatedBy string     `json:"created_by"`
	CreatedAt *time.Time `json:"created_at"`
}

type alertFailureResponse struct {
	IncidentID     int64      `json:"incident_id"`
	MonitorID      string     `json:"monitor_id"`
	Provider       string     `json:"provider"`
	AttemptedAt    *time.Time `json:"attempted_at"`
	AttemptNumber  int        `json:"attempt_number"`
	Error          string     `json:"error"`
	NextRetryAt    *time.Time `json:"next_retry_at"`
	RetryExhausted bool       `json:"retry_exhausted"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func allowGet(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet {
		return true
	}
	w.Header().Set("Allow", http.MethodGet)
	writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method not allowed"})
	return false
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func monitorResponses(states []storage.MonitorState, uptimeStats map[string]storage.UptimeStats, maintenance []storage.MaintenanceWindow, now time.Time) []monitorResponse {
	responses := make([]monitorResponse, 0, len(states))
	active, upcoming := maintenanceByMonitor(maintenance, now)
	for _, state := range states {
		stats := uptimeStats[state.MonitorID]
		responses = append(responses, monitorResponse{
			ID:                     state.MonitorID,
			Name:                   state.Name,
			URL:                    state.URL,
			Status:                 state.Status,
			ConsecutiveFailures:    state.ConsecutiveFailures,
			LastCheckedAt:          timePtr(state.LastCheckedAt),
			LastSuccessAt:          timePtr(state.LastSuccessAt),
			LastFailureAt:          timePtr(state.LastFailureAt),
			LastError:              state.LastError,
			LastObservedStatusCode: state.LastObservedStatusCode,
			UpdatedAt:              timePtr(state.UpdatedAt),
			Uptime:                 uptimeResponseFromStats(stats),
			ActiveMaintenance:      maintenanceResponsePtr(active[state.MonitorID]),
			UpcomingMaintenance:    maintenanceResponses(upcoming[state.MonitorID]),
		})
	}
	return responses
}

func observerResponseFromState(state storage.ObserverState, known bool, sentinels []storage.ObserverSentinelResult) observerResponse {
	status := "UNKNOWN"
	if known {
		status = state.Status
	}
	return observerResponse{
		Status:               status,
		ConsecutiveFailures:  state.ConsecutiveFailures,
		ConsecutiveSuccesses: state.ConsecutiveSuccesses,
		LastCheckedAt:        timePtr(state.LastCheckedAt),
		LastSuccessAt:        timePtr(state.LastSuccessAt),
		LastFailureAt:        timePtr(state.LastFailureAt),
		LastError:            state.LastError,
		UpdatedAt:            timePtr(state.UpdatedAt),
		Sentinels:            observerSentinelResponses(sentinels),
	}
}

func observerSentinelResponses(results []storage.ObserverSentinelResult) []observerSentinelResponse {
	responses := make([]observerSentinelResponse, 0, len(results))
	for _, result := range results {
		responses = append(responses, observerSentinelResponse{
			ID:                 result.SentinelID,
			Name:               result.Name,
			URL:                result.URL,
			OK:                 result.OK,
			ExpectedStatusCode: result.ExpectedStatusCode,
			ObservedStatusCode: result.ObservedStatusCode,
			LatencyMS:          result.LatencyMS,
			Error:              result.Error,
			CheckedAt:          timePtr(result.CheckedAt),
		})
	}
	return responses
}

func uptimeResponseFromStats(stats storage.UptimeStats) uptimeResponse {
	return uptimeResponse{
		TwentyFourHour: uptimeWindowResponseFromStats(stats.TwentyFourHour),
		SevenDay:       uptimeWindowResponseFromStats(stats.SevenDay),
		ThirtyDay:      uptimeWindowResponseFromStats(stats.ThirtyDay),
		Retained:       uptimeWindowResponseFromStats(stats.Retained),
	}
}

func uptimeWindowResponseFromStats(stats storage.UptimeWindowStats) uptimeWindowResponse {
	return uptimeWindowResponse{
		TotalChecks:             stats.TotalChecks,
		SuccessfulChecks:        stats.SuccessfulChecks,
		FailedChecks:            stats.FailedChecks,
		MaintenanceChecks:       stats.MaintenanceChecks,
		MaintenanceFailedChecks: stats.MaintenanceFailedChecks,
		DowntimeSeconds:         stats.DowntimeSeconds,
		ReportableSeconds:       stats.ReportableSeconds,
		UptimePercent:           uptimePercent(stats.DowntimeSeconds, stats.ReportableSeconds),
		WindowStartedAt:         timePtr(stats.WindowStartedAt),
		WindowEndedAt:           timePtr(stats.WindowEndedAt),
	}
}

func dailyHistoryResponseFromStats(states []storage.MonitorState, stats map[string][]storage.DailyUptimeStats, now time.Time) dailyHistoryResponse {
	now = now.UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	monitors := make([]monitorDailyHistoryResponse, 0, len(states))
	for _, state := range states {
		days := stats[state.MonitorID]
		responses := make([]dailyUptimeResponse, 0, len(days))
		for _, day := range days {
			responses = append(responses, dailyUptimeResponse{
				Date:              day.Date.UTC().Format("2006-01-02"),
				ReportableSeconds: day.ReportableSeconds,
				DowntimeSeconds:   day.DowntimeSeconds,
				UptimePercent:     uptimePercent(day.DowntimeSeconds, day.ReportableSeconds),
			})
		}
		monitors = append(monitors, monitorDailyHistoryResponse{ID: state.MonitorID, Name: state.Name, Days: responses})
	}
	return dailyHistoryResponse{
		Status:      "ok",
		GeneratedAt: timePtr(now),
		Timezone:    "UTC",
		Range: dailyHistoryRangeResponse{
			Days:      dailyHistoryDays,
			StartedOn: today.AddDate(0, 0, -(dailyHistoryDays - 1)).Format("2006-01-02"),
			EndedOn:   today.Format("2006-01-02"),
		},
		Monitors: monitors,
	}
}

func maintenanceByMonitor(windows []storage.MaintenanceWindow, now time.Time) (map[string]storage.MaintenanceWindow, map[string][]storage.MaintenanceWindow) {
	active := map[string]storage.MaintenanceWindow{}
	upcoming := map[string][]storage.MaintenanceWindow{}
	for _, window := range windows {
		if !window.CancelledAt.IsZero() || !window.EndsAt.After(now) {
			continue
		}
		if !now.Before(window.StartsAt) && now.Before(window.EndsAt) {
			active[window.MonitorID] = window
			continue
		}
		if now.Before(window.StartsAt) {
			upcoming[window.MonitorID] = append(upcoming[window.MonitorID], window)
		}
	}
	return active, upcoming
}

func maintenanceResponsePtr(window storage.MaintenanceWindow) *maintenanceResponse {
	if window.ID == 0 {
		return nil
	}
	response := maintenanceResponseFromWindow(window)
	return &response
}

func maintenanceResponses(windows []storage.MaintenanceWindow) []maintenanceResponse {
	responses := make([]maintenanceResponse, 0, len(windows))
	for _, window := range windows {
		responses = append(responses, maintenanceResponseFromWindow(window))
	}
	return responses
}

func maintenanceResponseFromWindow(window storage.MaintenanceWindow) maintenanceResponse {
	return maintenanceResponse{
		ID:        window.ID,
		StartsAt:  timePtr(window.StartsAt),
		EndsAt:    timePtr(window.EndsAt),
		Reason:    window.Reason,
		CreatedBy: window.CreatedBy,
		CreatedAt: timePtr(window.CreatedAt),
	}
}

func uptimePercent(downtimeSeconds int64, reportableSeconds int64) *float64 {
	if reportableSeconds == 0 {
		return nil
	}
	uptimeSeconds := reportableSeconds - downtimeSeconds
	if uptimeSeconds < 0 {
		uptimeSeconds = 0
	}
	percent := float64(uptimeSeconds) / float64(reportableSeconds) * 100
	rounded := math.Round(percent*100) / 100
	return &rounded
}

func alertFailureResponses(failures []storage.AlertNotification) []alertFailureResponse {
	responses := make([]alertFailureResponse, 0, len(failures))
	for _, failure := range failures {
		responses = append(responses, alertFailureResponse{
			IncidentID:     failure.IncidentID,
			MonitorID:      failure.MonitorID,
			Provider:       failure.Provider,
			AttemptedAt:    timePtr(failure.AttemptedAt),
			AttemptNumber:  failure.AttemptNumber,
			Error:          failure.Error,
			NextRetryAt:    timePtr(failure.NextRetryAt),
			RetryExhausted: failure.RetryExhausted,
		})
	}
	return responses
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	utc := t.UTC()
	return &utc
}
