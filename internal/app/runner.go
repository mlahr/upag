package app

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"upag/internal/alert"
	"upag/internal/checker"
	"upag/internal/config"
	"upag/internal/monitor"
	"upag/internal/state"
	"upag/internal/storage"
)

type Runner struct {
	configPath string
	store      *storage.Store
	out        io.Writer
	errOut     io.Writer

	mu      sync.Mutex
	logMu   sync.Mutex
	cfg     config.Config
	emailer alert.IncidentSender
	workers map[string]context.CancelFunc
}

func NewRunner(configPath string, cfg config.Config, store *storage.Store, out io.Writer, errOut io.Writer) (*Runner, error) {
	return &Runner{
		configPath: configPath,
		cfg:        cfg,
		store:      store,
		out:        out,
		errOut:     errOut,
		emailer:    alert.NewIncidentSender(cfg),
		workers:    map[string]context.CancelFunc{},
	}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	reloadCh := make(chan os.Signal, 1)
	signal.Notify(reloadCh, syscall.SIGHUP)
	defer signal.Stop(reloadCh)

	r.logInfo("daemon_start", "config=%q monitors=%d", r.configPath, len(r.cfg.Monitors))
	r.applyConfig(ctx, r.cfg)
	r.logInfo("daemon_ready", "config=%q monitors=%d", r.configPath, len(r.cfg.Monitors))
	pruneTicker := time.NewTicker(time.Hour)
	defer pruneTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.stopAll()
			r.logInfo("daemon_shutdown", "reason=%q", ctx.Err())
			return nil
		case <-reloadCh:
			cfg, err := config.LoadFile(r.configPath)
			if err != nil {
				r.logError("config_reload_failed", "error=%q", err)
				continue
			}
			r.applyConfig(ctx, cfg)
			r.logInfo("config_reloaded", "config=%q monitors=%d", r.configPath, len(cfg.Monitors))
		case <-pruneTicker.C:
			r.mu.Lock()
			retention := r.cfg.Defaults.HistoryRetention.Duration
			r.mu.Unlock()
			if err := r.store.PruneProbeResults(ctx, retention, time.Now().UTC()); err != nil {
				r.logError("history_prune_failed", "error=%q", err)
			}
		}
	}
}

func (r *Runner) applyConfig(parent context.Context, cfg config.Config) {
	r.mu.Lock()
	defer r.mu.Unlock()

	desired := map[string]config.MonitorConfig{}
	desiredIDs := make([]string, 0, len(cfg.Monitors))
	for _, mon := range cfg.Monitors {
		desired[mon.ID] = mon
		desiredIDs = append(desiredIDs, mon.ID)
	}
	for id, cancel := range r.workers {
		if _, ok := desired[id]; !ok {
			cancel()
			delete(r.workers, id)
		}
	}
	if err := r.store.DeleteStatesExcept(parent, desiredIDs); err != nil {
		r.logError("stale_monitor_cleanup_failed", "error=%q", err)
	}
	for _, mon := range cfg.Monitors {
		if cancel, ok := r.workers[mon.ID]; ok {
			cancel()
		}
		workerCtx, cancel := context.WithCancel(parent)
		r.workers[mon.ID] = cancel
		go r.runMonitor(workerCtx, cfg.Defaults.FailureThreshold, mon)
	}
	r.cfg = cfg
	r.emailer = alert.NewIncidentSender(cfg)
}

func (r *Runner) stopAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, cancel := range r.workers {
		cancel()
		delete(r.workers, id)
	}
}

func (r *Runner) runMonitor(ctx context.Context, threshold int, mon config.MonitorConfig) {
	r.probe(ctx, threshold, mon)
	ticker := time.NewTicker(mon.Interval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.probe(ctx, threshold, mon)
		}
	}
}

func (r *Runner) probe(ctx context.Context, threshold int, mon config.MonitorConfig) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	result := checker.Check(ctx, mon)
	now := result.CheckedAt
	previous, ok, err := r.store.GetState(ctx, mon.ID)
	if err != nil {
		r.logError("state_read_failed", "monitor_id=%q error=%q", mon.ID, err)
		return
	}
	if !ok {
		previous = storage.MonitorState{
			MonitorID:          mon.ID,
			Name:               mon.Name,
			URL:                mon.URL,
			ExpectedStatusCode: mon.ExpectedStatusCode,
			Status:             state.Unknown,
		}
	}
	previous.Name = mon.Name
	previous.URL = mon.URL
	previous.ExpectedStatusCode = mon.ExpectedStatusCode

	evaluation := monitor.Evaluate(previous, result, threshold, now)
	next := evaluation.NextState
	next.MonitorID = mon.ID
	next.Name = mon.Name
	next.URL = mon.URL
	next.ExpectedStatusCode = mon.ExpectedStatusCode
	next.UpdatedAt = now

	probeResult := storage.ProbeResult{
		MonitorID:          mon.ID,
		CheckedAt:          now,
		OK:                 result.OK,
		ObservedStatusCode: result.ObservedStatusCode,
		LatencyMS:          result.Latency.Milliseconds(),
		Error:              result.Error,
	}

	var incident *storage.Incident
	if evaluation.IncidentTransition != "" {
		incident = &storage.Incident{
			MonitorID:  mon.ID,
			Name:       mon.Name,
			Transition: evaluation.IncidentTransition,
			ObservedAt: now,
			Error:      result.Error,
			StatusCode: result.ObservedStatusCode,
		}
	}
	incidentID, err := r.store.SaveProbeAndState(ctx, probeResult, next, incident)
	if err != nil {
		r.logError("state_write_failed", "monitor_id=%q error=%q", mon.ID, err)
		return
	}
	r.logProbe(mon, result, next.Status)
	r.mu.Lock()
	emailer := r.emailer
	providers := strings.Join(emailer.Providers(), ",")
	r.mu.Unlock()
	if incident != nil {
		attemptedAt := time.Now().UTC()
		results := emailer.SendIncident(*incident, next)
		notifications := alertNotifications(incidentID, mon.ID, attemptedAt, results)
		if err := r.store.SaveAlertNotifications(ctx, notifications); err != nil {
			r.logError("alert_notification_write_failed", "monitor_id=%q incident_id=%d error=%q", mon.ID, incidentID, err)
		}
		if err := alert.SendResultsError(results); err != nil {
			r.logAlertDecision("error", mon, previous.Status, next.Status, evaluation.IncidentTransition, providers, false, "", err)
			return
		}
		r.logAlertDecision("info", mon, previous.Status, next.Status, evaluation.IncidentTransition, providers, true, "", nil)
		return
	}
	r.logAlertDecision("info", mon, previous.Status, next.Status, "", providers, false, alertSkipReason(previous, next, result, threshold), nil)
}

func (r *Runner) logProbe(mon config.MonitorConfig, result checker.Result, status string) {
	level := "info"
	if !result.OK {
		level = "error"
	}
	r.log(level, "probe_result", "monitor_id=%q name=%q url=%q ok=%t monitor_status=%q observed_status_code=%d expected_status_code=%d latency_ms=%d error=%q",
		mon.ID,
		mon.Name,
		mon.URL,
		result.OK,
		status,
		result.ObservedStatusCode,
		mon.ExpectedStatusCode,
		result.Latency.Milliseconds(),
		result.Error,
	)
}

func (r *Runner) logAlertDecision(level string, mon config.MonitorConfig, previousStatus string, status string, transition string, providers string, sent bool, reason string, err error) {
	format := "monitor_id=%q name=%q monitor_status=%q previous_status=%q transition=%q alert_sent=%t providers=%q reason=%q error=%q"
	errorText := ""
	if err != nil {
		errorText = err.Error()
	}
	r.log(level, "alert_decision", format, mon.ID, mon.Name, status, previousStatus, transition, sent, providers, reason, errorText)
}

func alertSkipReason(previous storage.MonitorState, next storage.MonitorState, result checker.Result, threshold int) string {
	if !result.OK && next.Status == state.Failing && next.ConsecutiveFailures < threshold {
		return "failure_threshold_not_reached"
	}
	if previous.Status == state.Down && next.Status == state.Down {
		return "already_down"
	}
	if previous.Status == state.Up && next.Status == state.Up {
		return "already_up"
	}
	return "no_state_transition"
}

func alertNotifications(incidentID int64, monitorID string, attemptedAt time.Time, results []alert.SendResult) []storage.AlertNotification {
	notifications := make([]storage.AlertNotification, 0, len(results))
	for _, result := range results {
		errorText := ""
		if result.Error != nil {
			errorText = result.Error.Error()
		}
		notifications = append(notifications, storage.AlertNotification{
			IncidentID:  incidentID,
			MonitorID:   monitorID,
			Provider:    result.Provider,
			AttemptedAt: attemptedAt,
			Success:     result.Error == nil,
			Error:       errorText,
		})
	}
	return notifications
}

func (r *Runner) logInfo(event string, format string, args ...any) {
	r.log("info", event, format, args...)
}

func (r *Runner) logError(event string, format string, args ...any) {
	r.log("error", event, format, args...)
}

func (r *Runner) log(level string, event string, format string, args ...any) {
	w := r.out
	if level == "error" {
		w = r.errOut
	}
	r.logMu.Lock()
	defer r.logMu.Unlock()
	fmt.Fprintf(w, "time=%q level=%s event=%s", time.Now().UTC().Format(time.RFC3339Nano), level, event)
	if format != "" {
		fmt.Fprint(w, " ")
		fmt.Fprintf(w, format, args...)
	}
	fmt.Fprintln(w)
}
