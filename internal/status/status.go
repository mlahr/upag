package status

import (
	"context"
	"encoding/json"
	"fmt"
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
	ListActionableAlertDeliveryFailures(ctx context.Context, limit int) ([]storage.AlertNotification, error)
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
			Monitors:              monitorResponses(states),
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
	ID                     string     `json:"id"`
	Name                   string     `json:"name"`
	URL                    string     `json:"url"`
	Status                 string     `json:"status"`
	ConsecutiveFailures    int        `json:"consecutive_failures"`
	LastCheckedAt          *time.Time `json:"last_checked_at"`
	LastSuccessAt          *time.Time `json:"last_success_at"`
	LastFailureAt          *time.Time `json:"last_failure_at"`
	LastError              string     `json:"last_error"`
	LastObservedStatusCode int        `json:"last_observed_status_code"`
	UpdatedAt              *time.Time `json:"updated_at"`
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

func monitorResponses(states []storage.MonitorState) []monitorResponse {
	responses := make([]monitorResponse, 0, len(states))
	for _, state := range states {
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
		})
	}
	return responses
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
