package app

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"upag/internal/alert"
	"upag/internal/checker"
	"upag/internal/config"
	"upag/internal/monitor"
	"upag/internal/observer"
	"upag/internal/state"
	httpstatus "upag/internal/status"
	"upag/internal/storage"
)

type Runner struct {
	configPath string
	version    string
	startedAt  time.Time
	store      storage.Backend
	out        io.Writer
	errOut     io.Writer

	mu            sync.Mutex
	logMu         sync.Mutex
	cfg           config.Config
	emailer       alert.IncidentSender
	workers       map[string]monitorWorker
	observer      observerWorker
	statusServer  *httpstatus.Server
	statusAddress string
	statusPort    int
}

type monitorWorker struct {
	cancel context.CancelFunc
	config monitorWorkerConfig
}

type monitorWorkerConfig struct {
	Monitor           config.MonitorConfig
	FailureThreshold  int
	ProbeRetries      int
	ProbeRetryBackoff time.Duration
}

type observerWorker struct {
	cancel context.CancelFunc
	config config.ObserverConfig
}

func NewRunner(configPath string, cfg config.Config, store storage.Backend, out io.Writer, errOut io.Writer, version string) (*Runner, error) {
	return &Runner{
		configPath: configPath,
		version:    version,
		startedAt:  time.Now().UTC(),
		cfg:        cfg,
		store:      store,
		out:        out,
		errOut:     errOut,
		emailer:    alert.NewIncidentSender(cfg),
		workers:    map[string]monitorWorker{},
	}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	reloadCh := make(chan os.Signal, 1)
	signal.Notify(reloadCh, syscall.SIGHUP)
	defer signal.Stop(reloadCh)

	r.logInfo("daemon_start", "config=%q monitors=%d", r.configPath, len(r.cfg.Monitors))
	r.applyConfig(ctx, r.cfg)
	if err := r.applyStatusServer(ctx, r.cfg.HTTP.Address, r.cfg.HTTP.Port); err != nil {
		return err
	}
	r.logInfo("daemon_ready", "config=%q monitors=%d", r.configPath, len(r.cfg.Monitors))
	pruneTicker := time.NewTicker(time.Hour)
	defer pruneTicker.Stop()
	retryTicker := time.NewTicker(30 * time.Second)
	defer retryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.stopAll()
			r.stopStatusServer()
			r.logInfo("daemon_shutdown", "reason=%q", ctx.Err())
			return nil
		case <-reloadCh:
			cfg, err := config.LoadFile(r.configPath)
			if err != nil {
				r.logError("config_reload_failed", "error=%q", err)
				continue
			}
			r.applyConfig(ctx, cfg)
			if err := r.applyStatusServer(ctx, cfg.HTTP.Address, cfg.HTTP.Port); err != nil {
				r.logError("status_http_reload_failed", "address=%q error=%q", httpstatus.ListenAddress(cfg.HTTP.Address, cfg.HTTP.Port), err)
				continue
			}
			r.logInfo("config_reloaded", "config=%q monitors=%d", r.configPath, len(cfg.Monitors))
		case <-pruneTicker.C:
			r.mu.Lock()
			retention := probeRetentionPolicy(r.cfg)
			r.mu.Unlock()
			if err := r.store.RollupAndPruneProbeResults(ctx, retention, time.Now().UTC()); err != nil {
				r.logError("history_prune_failed", "error=%q", err)
			}
		case <-retryTicker.C:
			r.retryAlertNotifications(ctx)
		}
	}
}

func probeRetentionPolicy(cfg config.Config) storage.ProbeRetentionPolicy {
	return storage.ProbeRetentionPolicy{
		ProbeResults:       rollupRetention(cfg.Storage.ProbeResults.Retention),
		ProbeMinuteRollups: rollupRetention(cfg.Storage.ProbeMinuteRollups.Retention),
		ProbeHourlyRollups: rollupRetention(cfg.Storage.ProbeHourlyRollups.Retention),
		ProbeDailyRollups:  rollupRetention(cfg.Storage.ProbeDailyRollups.Retention),
	}
}

func rollupRetention(retention config.RetentionDuration) storage.RollupRetention {
	return storage.RollupRetention{
		Duration: retention.Duration,
		Forever:  retention.Forever,
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
	for id, worker := range r.workers {
		if _, ok := desired[id]; !ok {
			worker.cancel()
			delete(r.workers, id)
		}
	}
	if err := r.store.DeleteStatesExcept(parent, desiredIDs); err != nil {
		r.logError("stale_monitor_cleanup_failed", "error=%q", err)
	}
	for _, mon := range cfg.Monitors {
		workerConfig := monitorWorkerConfig{
			Monitor:           mon,
			FailureThreshold:  cfg.Defaults.FailureThreshold,
			ProbeRetries:      cfg.Defaults.ProbeRetries,
			ProbeRetryBackoff: cfg.Defaults.ProbeRetryBackoff.Duration,
		}
		if worker, ok := r.workers[mon.ID]; ok {
			if reflect.DeepEqual(worker.config, workerConfig) {
				continue
			}
			worker.cancel()
		}
		workerCtx, cancel := context.WithCancel(parent)
		r.workers[mon.ID] = monitorWorker{cancel: cancel, config: workerConfig}
		go r.runMonitor(workerCtx, workerConfig.FailureThreshold, workerConfig.ProbeRetries, workerConfig.ProbeRetryBackoff, workerConfig.Monitor)
	}
	if cfg.Observer.Enabled {
		if r.observer.cancel == nil || !reflect.DeepEqual(r.observer.config, cfg.Observer) {
			if r.observer.cancel != nil {
				r.observer.cancel()
			}
			observerCtx, cancel := context.WithCancel(parent)
			r.observer = observerWorker{cancel: cancel, config: cfg.Observer}
			go r.runObserver(observerCtx, cfg.Observer)
		}
	} else if r.observer.cancel != nil {
		r.observer.cancel()
		r.observer = observerWorker{}
	}
	r.cfg = cfg
	r.emailer = alert.NewIncidentSender(cfg)
}

func (r *Runner) applyStatusServer(parent context.Context, address string, port int) error {
	r.mu.Lock()
	currentServer := r.statusServer
	currentAddress := r.statusAddress
	currentPort := r.statusPort
	r.mu.Unlock()
	if currentAddress == address && currentPort == port {
		return nil
	}

	var nextServer *httpstatus.Server
	if port > 0 {
		var err error
		nextServer, err = httpstatus.Start(parent, address, port, r.store, r.statusMetadata)
		if err != nil {
			return err
		}
	}

	if currentServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := currentServer.Shutdown(shutdownCtx); err != nil {
			cancel()
			if nextServer != nil {
				_ = nextServer.Shutdown(context.Background())
			}
			return err
		}
		cancel()
	}

	r.mu.Lock()
	r.statusServer = nextServer
	r.statusAddress = address
	r.statusPort = port
	r.mu.Unlock()
	if port > 0 {
		r.logInfo("status_http_start", "address=%q", httpstatus.ListenAddress(address, port))
	} else if currentPort > 0 {
		r.logInfo("status_http_stop", "address=%q", httpstatus.ListenAddress(currentAddress, currentPort))
	}
	return nil
}

func (r *Runner) stopStatusServer() {
	r.mu.Lock()
	server := r.statusServer
	address := r.statusAddress
	port := r.statusPort
	r.statusServer = nil
	r.statusAddress = ""
	r.statusPort = 0
	r.mu.Unlock()
	if server == nil {
		return
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		r.logError("status_http_shutdown_failed", "address=%q error=%q", httpstatus.ListenAddress(address, port), err)
	}
}

func (r *Runner) statusMetadata() httpstatus.Metadata {
	r.mu.Lock()
	defer r.mu.Unlock()
	return httpstatus.Metadata{
		Version:          r.version,
		StartedAt:        r.startedAt,
		ConfigPath:       r.configPath,
		MonitorCount:     len(r.cfg.Monitors),
		FailureThreshold: r.cfg.Defaults.FailureThreshold,
	}
}

func (r *Runner) stopAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, worker := range r.workers {
		worker.cancel()
		delete(r.workers, id)
	}
	if r.observer.cancel != nil {
		r.observer.cancel()
		r.observer = observerWorker{}
	}
}

func (r *Runner) runObserver(ctx context.Context, cfg config.ObserverConfig) {
	r.probeObserver(ctx, cfg)
	ticker := time.NewTicker(cfg.Interval.Duration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.probeObserver(ctx, cfg)
		}
	}
}

func (r *Runner) probeObserver(ctx context.Context, cfg config.ObserverConfig) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	now := time.Now().UTC()
	previous, ok, err := r.store.GetObserverState(ctx)
	if err != nil {
		r.logError("observer_state_read_failed", "error=%q", err)
		return
	}
	check := observer.Check(ctx, cfg, previous, ok, now)
	var incident *storage.Incident
	if check.IncidentTransition != "" {
		incident = &storage.Incident{
			MonitorID:  observer.MonitorID,
			Name:       observer.Name,
			Transition: check.IncidentTransition,
			ObservedAt: now,
			Error:      check.State.LastError,
		}
	}
	incidentID, err := r.store.SaveObserverCheck(ctx, check.State, check.SentinelResults, incident)
	if err != nil {
		r.logError("observer_state_write_failed", "error=%q", err)
		return
	}
	healthy := check.State.Status != state.ObserverDown
	r.logInfo("observer_check", "status=%q healthy=%t consecutive_failures=%d consecutive_successes=%d error=%q", check.State.Status, healthy, check.State.ConsecutiveFailures, check.State.ConsecutiveSuccesses, check.State.LastError)
	if incident == nil {
		return
	}
	r.logInfo("observer_transition", "transition=%q error=%q", incident.Transition, incident.Error)
	current := storage.MonitorState{
		MonitorID: observer.MonitorID,
		Name:      observer.Name,
		Status:    check.State.Status,
		LastError: check.State.LastError,
		UpdatedAt: now,
	}
	r.sendIncidentNotifications(ctx, incidentID, *incident, current)
}

func (r *Runner) runMonitor(ctx context.Context, threshold int, probeRetries int, probeRetryBackoff time.Duration, mon config.MonitorConfig) {
	if delay := monitorInitialDelay(mon.ID, mon.Interval.Duration); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
	r.probe(ctx, threshold, probeRetries, probeRetryBackoff, mon)
	ticker := time.NewTicker(mon.Interval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.probe(ctx, threshold, probeRetries, probeRetryBackoff, mon)
		}
	}
}

func monitorInitialDelay(monitorID string, interval time.Duration) time.Duration {
	if interval <= time.Nanosecond {
		return 0
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(monitorID))
	return time.Duration(hash.Sum64()%uint64(interval-time.Nanosecond)) + time.Nanosecond
}

func (r *Runner) probe(ctx context.Context, threshold int, probeRetries int, probeRetryBackoff time.Duration, mon config.MonitorConfig) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	result, attemptCount := r.checkWithRetries(ctx, mon, probeRetries, probeRetryBackoff)
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

	activeMaintenance, inMaintenance, err := r.store.ActiveMaintenanceWindow(ctx, mon.ID, now)
	if err != nil {
		r.logError("maintenance_window_query_failed", "monitor_id=%q error=%q", mon.ID, err)
		return
	}

	probeResult := storage.ProbeResult{
		MonitorID:          mon.ID,
		CheckedAt:          now,
		OK:                 result.OK,
		ObservedStatusCode: result.ObservedStatusCode,
		LatencyMS:          result.Latency.Milliseconds(),
		ResponseTimeMS:     result.ResponseTime.Milliseconds(),
		AttemptCount:       attemptCount,
		Error:              result.Error,
	}

	observerState, observerKnown, err := r.store.GetObserverState(ctx)
	if err != nil {
		r.logError("observer_state_read_failed", "monitor_id=%q error=%q", mon.ID, err)
		return
	}
	if observerKnown && observerState.Status == state.ObserverDown && !result.OK {
		probeResult.ObserverSuppressed = true
		if err := r.store.SaveProbeResult(ctx, probeResult); err != nil {
			r.logError("probe_result_write_failed", "monitor_id=%q error=%q", mon.ID, err)
			return
		}
		r.logProbe(mon, result, previous.Status)
		r.logInfo("probe_suppressed_observer_down", "monitor_id=%q name=%q observer_status=%q error=%q", mon.ID, mon.Name, observerState.Status, observerState.LastError)
		return
	}

	if inMaintenance {
		next := maintenanceState(previous, result, mon, now)
		probeResult.MaintenanceWindowID = activeMaintenance.ID
		if _, err := r.store.SaveProbeAndState(ctx, probeResult, next, nil); err != nil {
			r.logError("state_write_failed", "monitor_id=%q maintenance_window_id=%d error=%q", mon.ID, activeMaintenance.ID, err)
			return
		}
		r.logProbe(mon, result, next.Status)
		r.mu.Lock()
		providers := strings.Join(r.emailer.Providers(), ",")
		r.mu.Unlock()
		r.logAlertDecision("info", mon, previous.Status, next.Status, "", providers, false, "maintenance_active", nil)
		return
	}

	effectiveThreshold := threshold
	evaluationPrevious := previous
	if previous.Status == state.Maintenance && result.OK && previous.StatusBeforeMaintenance == state.Down {
		evaluationPrevious.Status = state.Down
	}
	if previous.Status == state.Maintenance && !result.OK {
		effectiveThreshold = 1
	}
	evaluation := monitor.Evaluate(evaluationPrevious, result, effectiveThreshold, now)
	next := evaluation.NextState
	next.MonitorID = mon.ID
	next.Name = mon.Name
	next.URL = mon.URL
	next.ExpectedStatusCode = mon.ExpectedStatusCode
	next.StatusBeforeMaintenance = ""
	next.UpdatedAt = now

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
		if err := r.sendIncidentNotifications(ctx, incidentID, *incident, next); err != nil {
			r.logAlertDecision("error", mon, previous.Status, next.Status, evaluation.IncidentTransition, providers, false, "", err)
			return
		}
		r.logAlertDecision("info", mon, previous.Status, next.Status, evaluation.IncidentTransition, providers, true, "", nil)
		return
	}
	r.logAlertDecision("info", mon, previous.Status, next.Status, "", providers, false, alertSkipReason(previous, next, result, threshold), nil)
}

func (r *Runner) sendIncidentNotifications(ctx context.Context, incidentID int64, incident storage.Incident, current storage.MonitorState) error {
	r.mu.Lock()
	emailer := r.emailer
	r.mu.Unlock()
	attemptedAt := time.Now().UTC()
	results := emailer.SendIncident(incident, current)
	notifications := alertNotifications(incidentID, incident.MonitorID, attemptedAt, 1, r.retryPolicy(), results)
	if err := r.store.SaveAlertNotifications(ctx, notifications); err != nil {
		r.logError("alert_notification_write_failed", "monitor_id=%q incident_id=%d error=%q", incident.MonitorID, incidentID, err)
	}
	if err := alert.SendResultsError(results); err != nil {
		return err
	}
	return nil
}

func (r *Runner) checkWithRetries(ctx context.Context, mon config.MonitorConfig, probeRetries int, probeRetryBackoff time.Duration) (checker.Result, int) {
	if probeRetries < 0 {
		probeRetries = 0
	}
	attempts := probeRetries + 1
	var result checker.Result
	for attempt := 1; attempt <= attempts; attempt++ {
		result = checker.Check(ctx, mon)
		if result.OK || attempt == attempts {
			return result, attempt
		}
		if probeRetryBackoff <= 0 {
			continue
		}
		timer := time.NewTimer(probeRetryBackoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, attempt
		case <-timer.C:
		}
	}
	return result, attempts
}

func (r *Runner) retryAlertNotifications(ctx context.Context) {
	now := time.Now().UTC()
	due, err := r.store.ListDueAlertNotificationRetries(ctx, now, 50)
	if err != nil {
		r.logError("alert_retry_query_failed", "error=%q", err)
		return
	}
	if len(due) == 0 {
		return
	}

	r.mu.Lock()
	emailer := r.emailer
	policy := r.cfg.Alerts.NotificationRetries
	r.mu.Unlock()
	providers := emailer.Providers()

	for _, retry := range due {
		attemptNumber := retry.Notification.AttemptNumber + 1
		attemptedAt := time.Now().UTC()
		var result alert.SendResult
		if stringInSlice(retry.Notification.Provider, providers) {
			result = emailer.SendProvider(retry.Notification.Provider, retry.Incident, retry.CurrentState)
		} else {
			result = alert.SendResult{
				Provider: retry.Notification.Provider,
				Error:    fmt.Errorf("alert provider %q is not configured", retry.Notification.Provider),
			}
		}
		notification := alertNotification(retry.Notification.IncidentID, retry.Notification.MonitorID, attemptedAt, attemptNumber, policy, result)
		if !stringInSlice(retry.Notification.Provider, providers) {
			notification.RetryExhausted = true
			notification.NextRetryAt = time.Time{}
		}
		if err := r.store.SaveAlertNotifications(ctx, []storage.AlertNotification{notification}); err != nil {
			r.logError("alert_notification_write_failed", "monitor_id=%q incident_id=%d provider=%q error=%q", retry.Notification.MonitorID, retry.Notification.IncidentID, retry.Notification.Provider, err)
			continue
		}
		if result.Error != nil {
			r.logError("alert_retry_failed", "monitor_id=%q incident_id=%d provider=%q attempt_number=%d error=%q", retry.Notification.MonitorID, retry.Notification.IncidentID, retry.Notification.Provider, attemptNumber, result.Error)
			continue
		}
		r.logInfo("alert_retry_succeeded", "monitor_id=%q incident_id=%d provider=%q attempt_number=%d", retry.Notification.MonitorID, retry.Notification.IncidentID, retry.Notification.Provider, attemptNumber)
	}
}

func (r *Runner) logProbe(mon config.MonitorConfig, result checker.Result, status string) {
	level := "info"
	if !result.OK {
		level = "error"
	}
	r.log(level, "probe_result", "monitor_id=%q name=%q url=%q ok=%t monitor_status=%q observed_status_code=%d expected_status_code=%d latency_ms=%d response_time_ms=%d error=%q",
		mon.ID,
		mon.Name,
		mon.URL,
		result.OK,
		status,
		result.ObservedStatusCode,
		mon.ExpectedStatusCode,
		result.Latency.Milliseconds(),
		result.ResponseTime.Milliseconds(),
		result.Error,
	)
}

func maintenanceState(previous storage.MonitorState, result checker.Result, mon config.MonitorConfig, now time.Time) storage.MonitorState {
	next := previous
	next.MonitorID = mon.ID
	next.Name = mon.Name
	next.URL = mon.URL
	next.ExpectedStatusCode = mon.ExpectedStatusCode
	if previous.Status == state.Maintenance {
		next.StatusBeforeMaintenance = previous.StatusBeforeMaintenance
	} else {
		next.StatusBeforeMaintenance = previous.Status
	}
	next.Status = state.Maintenance
	next.ConsecutiveFailures = 0
	next.LastCheckedAt = now
	next.LastObservedStatusCode = result.ObservedStatusCode
	next.UpdatedAt = now
	if result.OK {
		next.LastSuccessAt = now
		next.LastError = ""
		return next
	}
	next.LastFailureAt = now
	next.LastError = checker.FailureMessage(result)
	return next
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
		return "alert_already_sent"
	}
	return "no_state_transition"
}

func alertNotifications(incidentID int64, monitorID string, attemptedAt time.Time, attemptNumber int, policy config.NotificationRetriesConfig, results []alert.SendResult) []storage.AlertNotification {
	notifications := make([]storage.AlertNotification, 0, len(results))
	for _, result := range results {
		notifications = append(notifications, alertNotification(incidentID, monitorID, attemptedAt, attemptNumber, policy, result))
	}
	return notifications
}

func alertNotification(incidentID int64, monitorID string, attemptedAt time.Time, attemptNumber int, policy config.NotificationRetriesConfig, result alert.SendResult) storage.AlertNotification {
	errorText := ""
	if result.Error != nil {
		errorText = result.Error.Error()
	}
	notification := storage.AlertNotification{
		IncidentID:    incidentID,
		MonitorID:     monitorID,
		Provider:      result.Provider,
		AttemptedAt:   attemptedAt,
		AttemptNumber: attemptNumber,
		Success:       result.Error == nil,
		Error:         errorText,
	}
	if result.Error != nil {
		if attemptNumber >= policy.MaxAttempts {
			notification.RetryExhausted = true
		} else {
			notification.NextRetryAt = attemptedAt.Add(retryBackoff(policy, attemptNumber))
		}
	}
	return notification
}

func retryBackoff(policy config.NotificationRetriesConfig, attemptNumber int) time.Duration {
	if len(policy.Backoff) == 0 {
		return 0
	}
	index := attemptNumber - 1
	if index < 0 {
		index = 0
	}
	if index >= len(policy.Backoff) {
		index = len(policy.Backoff) - 1
	}
	return policy.Backoff[index].Duration
}

func (r *Runner) retryPolicy() config.NotificationRetriesConfig {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cfg.Alerts.NotificationRetries
}

func stringInSlice(value string, values []string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
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
