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
	"upag/internal/controlapi"
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

	mu                 sync.Mutex
	logMu              sync.Mutex
	transferMu         sync.Mutex
	alertDispatchMu    sync.Mutex
	cfg                config.Config
	transferLastSample time.Time
	transferSlowStreak int
	transferDegraded   bool
	pendingAlertWrites map[int64]pendingMonitorAlertWrite
	emailer            alert.IncidentSender
	workers            map[string]monitorWorker
	observer           observerWorker
	statusServer       *httpstatus.Server
	statusAddress      string
	statusPort         int
	statusTenant       string
	tenantID           string
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

type pendingMonitorAlertWrite struct {
	notifications []storage.AlertNotification
	deliveredAt   time.Time
	incident      storage.Incident
	sendErr       error
}

var statusServerShutdownTimeout = 5 * time.Second

func NewRunner(configPath string, cfg config.Config, store storage.Backend, out io.Writer, errOut io.Writer, version string) (*Runner, error) {
	return &Runner{
		configPath:         configPath,
		version:            version,
		startedAt:          time.Now().UTC(),
		cfg:                cfg,
		store:              store,
		out:                out,
		errOut:             errOut,
		emailer:            alert.NewIncidentSender(cfg),
		workers:            map[string]monitorWorker{},
		pendingAlertWrites: map[int64]pendingMonitorAlertWrite{},
		tenantID:           cfg.TenantID,
		statusTenant:       cfg.TenantID,
	}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	reloadCh := make(chan os.Signal, 1)
	signal.Notify(reloadCh, syscall.SIGHUP)
	defer signal.Stop(reloadCh)

	r.logInfo("daemon_start", "config=%q tenant=%q monitors=%d", r.configPath, r.cfg.TenantID, len(r.cfg.Monitors))
	r.applyConfig(ctx, r.cfg)
	if err := r.applyStatusServer(ctx, r.cfg.HTTP.Address, r.cfg.HTTP.Port, r.cfg.TenantID); err != nil {
		return err
	}
	r.logInfo("daemon_ready", "config=%q tenant=%q monitors=%d", r.configPath, r.cfg.TenantID, len(r.cfg.Monitors))
	r.retryAlertNotifications(ctx)
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
			if err := r.applyStatusServer(ctx, cfg.HTTP.Address, cfg.HTTP.Port, cfg.TenantID); err != nil {
				r.logError("status_http_reload_failed", "address=%q error=%q", httpstatus.ListenAddress(cfg.HTTP.Address, cfg.HTTP.Port), err)
				continue
			}
			r.logInfo("config_reloaded", "config=%q monitors=%d", r.configPath, len(cfg.Monitors))
		case <-pruneTicker.C:
			r.mu.Lock()
			retention := probeRetentionPolicy(r.cfg)
			connectivityRetention := r.cfg.Storage.ConnectivitySamples.Retention
			r.mu.Unlock()
			if err := r.store.RollupAndPruneProbeResults(r.storageContext(ctx), retention, time.Now().UTC()); err != nil {
				r.logError("history_prune_failed", "error=%q", err)
			}
			if !connectivityRetention.Forever {
				cutoff := time.Now().UTC().Add(-connectivityRetention.Duration)
				if err := r.store.PruneConnectivitySamples(r.storageContext(ctx), cutoff); err != nil {
					r.logError("connectivity_history_prune_failed", "error=%q", err)
				}
			}
		case <-retryTicker.C:
			r.retryAlertNotifications(ctx)
		}
	}
}

func (r *Runner) storageContext(parent context.Context) context.Context {
	r.mu.Lock()
	tenantID := r.tenantID
	r.mu.Unlock()
	return storage.WithTenant(parent, tenantID)
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
	r.tenantID = cfg.TenantID
	if err := r.store.EnsureStatusIntervalsBackfilled(storage.WithTenant(parent, cfg.TenantID), failureThresholds(cfg)); err != nil {
		r.logError("status_interval_backfill_failed", "tenant=%q error=%q", cfg.TenantID, err)
	}

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
	if err := r.store.DeleteStatesExcept(storage.WithTenant(parent, cfg.TenantID), desiredIDs); err != nil {
		r.logError("stale_monitor_cleanup_failed", "error=%q", err)
	}
	for _, mon := range cfg.Monitors {
		workerConfig := monitorWorkerConfig{
			Monitor:           mon,
			FailureThreshold:  mon.FailureThreshold,
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

func (r *Runner) applyStatusServer(parent context.Context, address string, port int, tenantID string) error {
	r.mu.Lock()
	currentServer := r.statusServer
	currentAddress := r.statusAddress
	currentPort := r.statusPort
	currentTenant := r.statusTenant
	r.mu.Unlock()
	if currentAddress == address && currentPort == port && currentTenant == tenantID {
		return nil
	}

	var nextServer *httpstatus.Server
	if port > 0 {
		var err error
		controlHandler := controlapi.NewHandler(r.store, r.controlRuntime)
		nextServer, err = httpstatus.Start(parent, address, port, r.store, tenantID, r.statusMetadata, controlHandler)
		if err != nil {
			return err
		}
	}

	if currentServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), statusServerShutdownTimeout)
		if err := currentServer.Shutdown(shutdownCtx); err != nil {
			cancel()
			closeErr := currentServer.Close()
			closeError := ""
			if closeErr != nil {
				closeError = closeErr.Error()
			}
			r.logError("status_http_shutdown_forced", "address=%q error=%q close_error=%q", httpstatus.ListenAddress(currentAddress, currentPort), err, closeError)
		} else {
			cancel()
		}
	}

	r.mu.Lock()
	r.statusServer = nextServer
	r.statusAddress = address
	r.statusPort = port
	r.statusTenant = tenantID
	r.mu.Unlock()
	if port > 0 {
		r.logInfo("status_http_start", "address=%q tenant=%q", httpstatus.ListenAddress(address, port), tenantID)
	} else if currentPort > 0 {
		r.logInfo("status_http_stop", "address=%q tenant=%q", httpstatus.ListenAddress(currentAddress, currentPort), currentTenant)
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
	r.statusTenant = ""
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
		Version:           r.version,
		StartedAt:         r.startedAt,
		ConfigPath:        r.configPath,
		MonitorCount:      len(r.cfg.Monitors),
		FailureThresholds: failureThresholds(r.cfg),
	}
}

func (r *Runner) controlRuntime() controlapi.Runtime {
	r.mu.Lock()
	defer r.mu.Unlock()
	monitors := make([]config.MonitorConfig, len(r.cfg.Monitors))
	copy(monitors, r.cfg.Monitors)
	return controlapi.Runtime{
		Version:           r.version,
		StartedAt:         r.startedAt,
		TenantID:          r.cfg.TenantID,
		BearerToken:       r.cfg.HTTP.Auth.BearerToken,
		Monitors:          monitors,
		FailureThresholds: failureThresholds(r.cfg),
		TransferCheck:     r.cfg.Observer.TransferCheck,
	}
}

func failureThresholds(cfg config.Config) storage.FailureThresholds {
	thresholds := storage.FailureThresholds{
		Default:  cfg.Defaults.FailureThreshold,
		Monitors: map[string]int{},
	}
	for _, mon := range cfg.Monitors {
		if mon.FailureThreshold > 0 {
			thresholds.Monitors[mon.ID] = mon.FailureThreshold
		}
	}
	return thresholds
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
	r.restoreTransferGuard(ctx, cfg.TransferCheck)
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

func (r *Runner) restoreTransferGuard(ctx context.Context, cfg config.TransferCheckConfig) {
	if !cfg.Enabled {
		return
	}
	limit := cfg.ConsecutiveSlowResults
	if limit <= 0 {
		limit = 2
	}
	now := time.Now().UTC()
	freshnessWindow := transferGuardFreshnessWindow(cfg)
	samples, err := r.store.ListConnectivitySamples(r.storageContext(ctx), storage.ConnectivitySampleFilter{
		Kind: "transfer", Since: now.Add(-freshnessWindow), Limit: 1000,
	})
	if err != nil {
		r.logError("transfer_guard_restore_failed", "error=%q", err)
		return
	}
	r.transferMu.Lock()
	defer r.transferMu.Unlock()
	r.transferSlowStreak = 0
	r.transferDegraded = false
	r.transferLastSample = time.Time{}
	if len(samples) > 0 {
		r.transferLastSample = samples[0].CheckedAt
		if now.Sub(r.transferLastSample) > freshnessWindow {
			return
		}
		maxStreakGap := transferGuardMaxStreakGap(cfg)
		var previousAt time.Time
		for i := len(samples) - 1; i >= 0; i-- {
			sample := samples[i]
			if !previousAt.IsZero() && sample.CheckedAt.Sub(previousAt) > maxStreakGap {
				r.transferSlowStreak = 0
			}
			switch {
			case transferSampleIsSlow(sample):
				r.transferSlowStreak++
				if r.transferSlowStreak >= limit {
					r.transferDegraded = true
				}
			case sample.OK:
				r.transferSlowStreak = 0
				r.transferDegraded = false
			default:
				// An unexpected HTTP status breaks a slow streak but does not
				// confirm that transfer performance has recovered.
				r.transferSlowStreak = 0
			}
			previousAt = sample.CheckedAt
		}
	}
}

func transferGuardFreshnessWindow(cfg config.TransferCheckConfig) time.Duration {
	interval := cfg.Interval.Duration
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	return 2 * interval
}

func transferGuardMaxStreakGap(cfg config.TransferCheckConfig) time.Duration {
	if cfg.Interval.Duration <= 0 {
		return 15 * time.Minute
	}
	return cfg.Interval.Duration
}

func (r *Runner) sampleTransfer(ctx context.Context, cfg config.TransferCheckConfig, source string, monitorID string) bool {
	if !cfg.Enabled {
		return false
	}
	r.transferMu.Lock()
	defer r.transferMu.Unlock()
	now := time.Now().UTC()
	minimumInterval := cfg.IncidentInterval.Duration
	if source == "scheduled" {
		minimumInterval = cfg.Interval.Duration
	}
	if minimumInterval <= 0 {
		minimumInterval = 30 * time.Second
	}
	if !r.transferLastSample.IsZero() && now.Sub(r.transferLastSample) < minimumInterval {
		return r.transferDegraded
	}
	consecutiveSlowResults := cfg.ConsecutiveSlowResults
	if consecutiveSlowResults <= 0 {
		consecutiveSlowResults = 2
	}
	sample := observer.CheckTransfer(ctx, cfg, source, monitorID, now)
	if ctx.Err() != nil {
		return r.transferDegraded
	}
	if err := r.store.SaveConnectivitySamples(r.storageContext(ctx), []storage.ConnectivitySample{sample}); err != nil {
		r.logError("connectivity_sample_write_failed", "kind=%q source=%q error=%q", "transfer", source, err)
		return r.transferDegraded
	}
	if !r.transferLastSample.IsZero() && sample.CheckedAt.Sub(r.transferLastSample) > transferGuardMaxStreakGap(cfg) {
		r.transferSlowStreak = 0
	}
	r.transferLastSample = sample.CheckedAt
	if transferSampleIsSlow(sample) {
		r.transferSlowStreak++
		if r.transferSlowStreak >= consecutiveSlowResults {
			r.transferDegraded = true
		}
	} else if sample.OK {
		r.transferSlowStreak = 0
		r.transferDegraded = false
	} else {
		// An unexpected HTTP status says nothing about transfer speed.
		r.transferSlowStreak = 0
	}
	r.logInfo("transfer_check", "source=%q monitor_id=%q ok=%t slow=%t response_time_ms=%d bytes_read=%d error=%q degraded=%t consecutive_slow=%d",
		source, monitorID, sample.OK, sample.Slow, sample.ResponseTimeMS, sample.BytesRead, sample.Error, r.transferDegraded, r.transferSlowStreak)
	return r.transferDegraded
}

func transferSampleIsSlow(sample storage.ConnectivitySample) bool {
	return sample.Slow && (sample.ObservedStatusCode == 0 || sample.ObservedStatusCode == sample.ExpectedStatusCode)
}

func (r *Runner) probeObserver(ctx context.Context, cfg config.ObserverConfig) {
	ctx = r.storageContext(ctx)
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
	sentinelSamples := make([]storage.ConnectivitySample, 0, len(check.SentinelResults))
	for _, result := range check.SentinelResults {
		sentinelSamples = append(sentinelSamples, storage.ConnectivitySample{
			Kind: "sentinel", Source: "scheduled", TargetID: result.SentinelID, TargetName: result.Name,
			URL: result.URL, MonitorID: observer.MonitorID, OK: result.OK,
			ExpectedStatusCode: result.ExpectedStatusCode, ObservedStatusCode: result.ObservedStatusCode,
			LatencyMS: result.LatencyMS, ResponseTimeMS: result.ResponseTimeMS, Error: result.Error,
			CheckedAt: result.CheckedAt,
		})
	}
	if err := r.store.SaveConnectivitySamples(ctx, sentinelSamples); err != nil {
		r.logError("connectivity_sample_write_failed", "kind=%q error=%q", "sentinel", err)
	}
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
	if cfg.TransferCheck.Enabled {
		r.sampleTransfer(ctx, cfg.TransferCheck, "scheduled", "")
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
	recorded, sendErr := r.sendIncidentNotificationsTracked(ctx, incidentID, *incident, current)
	if !recorded {
		return
	}
	if sendErr != nil {
		r.logError("observer_alert_dispatch_failed", "incident_id=%d transition=%q error=%q", incidentID, incident.Transition, sendErr)
	}
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
	ctx = r.storageContext(ctx)
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
		FailureKind:        string(result.FailureKind),
	}

	observerState, observerKnown, err := r.store.GetObserverState(ctx)
	if err != nil {
		r.logError("observer_state_read_failed", "monitor_id=%q error=%q", mon.ID, err)
		return
	}
	if observerKnown && observerState.Status == state.ObserverDown && !result.OK {
		probeResult.ObserverSuppressed = true
		probeResult.SuppressionReason = "observer_down"
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

	// LastError reports a failed sentinel quorum. Partial sentinel failures are
	// cleared when the observer still meets its required-success quorum.
	if !result.OK && observerKnown && observerState.Status != state.ObserverDown && observerState.LastError == "" && checker.IsConnectivitySuppressible(result.FailureKind) {
		r.mu.Lock()
		transferCfg := r.cfg.Observer.TransferCheck
		r.mu.Unlock()
		if r.sampleTransfer(ctx, transferCfg, "incident", mon.ID) {
			probeResult.ConnectivitySuppressed = true
			probeResult.SuppressionReason = "cloudflare_transfer_slow"
			if err := r.store.SaveProbeResult(ctx, probeResult); err != nil {
				r.logError("probe_result_write_failed", "monitor_id=%q error=%q", mon.ID, err)
				return
			}
			r.logProbe(mon, result, previous.Status)
			r.logInfo("probe_suppressed_connectivity_degraded", "monitor_id=%q name=%q failure_kind=%q transfer_guard=degraded", mon.ID, mon.Name, result.FailureKind)
			return
		}
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
	delay := time.Duration(0)
	if incident != nil {
		r.mu.Lock()
		switch incident.Transition {
		case state.Down:
			delay = r.cfg.Alerts.TransitionDelay.Down.Duration
		case state.Up:
			delay = r.cfg.Alerts.TransitionDelay.Up.Duration
		}
		r.mu.Unlock()
	}
	dueAt := time.Now().UTC().Add(delay)
	incidentID, alertQueued, err := r.store.SaveProbeAndStateWithPendingAlert(ctx, probeResult, next, incident, dueAt)
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
		if !alertQueued {
			r.logInfo("monitor_alert_cancelled", "monitor_id=%q transition=%q reason=%q", mon.ID, incident.Transition, "monitor returned to last notified state")
			return
		}
		if delay == 0 {
			r.dispatchPendingMonitorAlerts(ctx)
		} else {
			r.logInfo("monitor_alert_queued", "monitor_id=%q incident_id=%d transition=%q delay=%q due_at=%q providers=%q", mon.ID, incidentID, incident.Transition, delay, dueAt.Format(time.RFC3339Nano), providers)
		}
		return
	}
	r.logAlertDecision("info", mon, previous.Status, next.Status, "", providers, false, alertSkipReason(previous, next, result, threshold), nil)
}

func (r *Runner) sendIncidentNotificationsTracked(ctx context.Context, incidentID int64, incident storage.Incident, current storage.MonitorState) (bool, error) {
	notifications, sendErr := r.incidentNotificationRecords(incidentID, incident, current)
	if err := r.store.SaveAlertNotifications(r.storageContext(ctx), notifications); err != nil {
		r.logError("alert_notification_write_failed", "monitor_id=%q incident_id=%d error=%q", incident.MonitorID, incidentID, err)
		return false, err
	}
	return true, sendErr
}

func (r *Runner) incidentNotificationRecords(incidentID int64, incident storage.Incident, current storage.MonitorState) ([]storage.AlertNotification, error) {
	r.mu.Lock()
	emailer := r.emailer
	r.mu.Unlock()
	attemptedAt := time.Now().UTC()
	results := emailer.SendIncident(incident, current)
	notifications := alertNotifications(incidentID, incident.MonitorID, attemptedAt, 1, r.retryPolicy(), results)
	return notifications, alert.SendResultsError(results)
}

func (r *Runner) dispatchPendingMonitorAlerts(ctx context.Context) {
	r.alertDispatchMu.Lock()
	defer r.alertDispatchMu.Unlock()
	if r.pendingAlertWrites == nil {
		r.pendingAlertWrites = map[int64]pendingMonitorAlertWrite{}
	}
	r.retryPendingMonitorAlertWrites(ctx)
	due, err := r.store.ListDuePendingMonitorAlerts(r.storageContext(ctx), time.Now().UTC(), 50)
	if err != nil {
		r.logError("pending_monitor_alert_query_failed", "error=%q", err)
		return
	}
	for _, pending := range due {
		claimed, err := r.store.ClaimPendingMonitorAlert(r.storageContext(ctx), pending.ID)
		if err != nil {
			r.logError("pending_monitor_alert_claim_failed", "monitor_id=%q incident_id=%d error=%q", pending.Incident.MonitorID, pending.Incident.ID, err)
			continue
		}
		if !claimed {
			continue
		}
		notifications, sendErr := r.incidentNotificationRecords(pending.Incident.ID, pending.Incident, pending.CurrentState)
		delivery := pendingMonitorAlertWrite{
			notifications: notifications,
			deliveredAt:   time.Now().UTC(),
			incident:      pending.Incident,
			sendErr:       sendErr,
		}
		if err := r.store.RecordPendingMonitorAlertDelivery(r.storageContext(ctx), pending.ID, delivery.notifications, delivery.deliveredAt); err != nil {
			r.pendingAlertWrites[pending.ID] = delivery
			r.logError("pending_monitor_alert_record_failed", "monitor_id=%q incident_id=%d error=%q", pending.Incident.MonitorID, pending.Incident.ID, err)
			continue
		}
		r.logPendingMonitorAlertOutcome(delivery)
	}
}

func (r *Runner) retryPendingMonitorAlertWrites(ctx context.Context) {
	for id, delivery := range r.pendingAlertWrites {
		if err := r.store.RecordPendingMonitorAlertDelivery(r.storageContext(ctx), id, delivery.notifications, delivery.deliveredAt); err != nil {
			r.logError("pending_monitor_alert_record_retry_failed", "monitor_id=%q incident_id=%d error=%q", delivery.incident.MonitorID, delivery.incident.ID, err)
			continue
		}
		delete(r.pendingAlertWrites, id)
		r.logPendingMonitorAlertOutcome(delivery)
	}
}

func (r *Runner) logPendingMonitorAlertOutcome(delivery pendingMonitorAlertWrite) {
	if delivery.sendErr != nil {
		r.logError("monitor_alert_dispatch_failed", "monitor_id=%q incident_id=%d transition=%q error=%q", delivery.incident.MonitorID, delivery.incident.ID, delivery.incident.Transition, delivery.sendErr)
		return
	}
	r.logInfo("monitor_alert_dispatched", "monitor_id=%q incident_id=%d transition=%q", delivery.incident.MonitorID, delivery.incident.ID, delivery.incident.Transition)
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
	r.dispatchPendingMonitorAlerts(ctx)
	now := time.Now().UTC()
	due, err := r.store.ListDueAlertNotificationRetries(r.storageContext(ctx), now, 50)
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
		if err := r.store.SaveAlertNotifications(r.storageContext(ctx), []storage.AlertNotification{notification}); err != nil {
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
	r.log(level, "probe_result", "monitor_id=%q name=%q url=%q ok=%t monitor_status=%q observed_status_code=%d expected_status_code=%d latency_ms=%d response_time_ms=%d failure_kind=%q error=%q",
		mon.ID,
		mon.Name,
		mon.URL,
		result.OK,
		status,
		result.ObservedStatusCode,
		mon.ExpectedStatusCode,
		result.Latency.Milliseconds(),
		result.ResponseTime.Milliseconds(),
		result.FailureKind,
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
