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

const alertFailureLimit = 50

type Store interface {
	ListStates(ctx context.Context) ([]storage.MonitorState, error)
	ListUptimeStats(ctx context.Context, now time.Time) (map[string]storage.UptimeStats, error)
	ListActionableAlertDeliveryFailures(ctx context.Context, limit int) ([]storage.AlertNotification, error)
	ListMaintenanceWindows(ctx context.Context, filter storage.MaintenanceWindowFilter) ([]storage.MaintenanceWindow, error)
}

type Metadata struct {
	Version      string
	StartedAt    time.Time
	ConfigPath   string
	MonitorCount int
}

type MetadataProvider func() Metadata

type Server struct {
	server       *http.Server
	cancel       context.CancelFunc
	shutdownOnce sync.Once
	shutdownErr  error
}

func Start(ctx context.Context, address string, port int, store Store, metadata MetadataProvider) (*Server, error) {
	listenAddress := ListenAddress(address, port)
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", listenAddress, err)
	}

	server := &http.Server{
		Handler: NewHandler(store, metadata),
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

func NewHandler(store Store, metadata MetadataProvider) http.Handler {
	mux := http.NewServeMux()
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
		states, err := store.ListStates(ctx)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
			return
		}
		now := time.Now().UTC()
		uptimeStats, err := store.ListUptimeStats(ctx, now)
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
		meta := metadata()
		writeJSON(w, http.StatusOK, statusResponse{
			Status:                "ok",
			Version:               meta.Version,
			StartedAt:             timePtr(meta.StartedAt),
			ConfigPath:            meta.ConfigPath,
			MonitorCount:          meta.MonitorCount,
			Monitors:              monitorResponses(states, uptimeStats, maintenance, now),
			AlertDeliveryFailures: alertFailureResponses(failures),
		})
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
	Monitors              []monitorResponse      `json:"monitors"`
	AlertDeliveryFailures []alertFailureResponse `json:"alert_delivery_failures"`
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
	UptimePercent           *float64   `json:"uptime_percent"`
	WindowStartedAt         *time.Time `json:"window_started_at"`
	WindowEndedAt           *time.Time `json:"window_ended_at"`
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
		UptimePercent:           uptimePercent(stats.SuccessfulChecks, stats.TotalChecks),
		WindowStartedAt:         timePtr(stats.WindowStartedAt),
		WindowEndedAt:           timePtr(stats.WindowEndedAt),
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

func uptimePercent(successfulChecks int, totalChecks int) *float64 {
	if totalChecks == 0 {
		return nil
	}
	percent := float64(successfulChecks) / float64(totalChecks) * 100
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
