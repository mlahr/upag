package app

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
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
	cfg     config.Config
	emailer *alert.Emailer
	workers map[string]context.CancelFunc
}

func NewRunner(configPath string, cfg config.Config, store *storage.Store, out io.Writer, errOut io.Writer) (*Runner, error) {
	return &Runner{
		configPath: configPath,
		cfg:        cfg,
		store:      store,
		out:        out,
		errOut:     errOut,
		emailer:    alert.NewEmailer(cfg.SMTP),
		workers:    map[string]context.CancelFunc{},
	}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	reloadCh := make(chan os.Signal, 1)
	signal.Notify(reloadCh, syscall.SIGHUP)
	defer signal.Stop(reloadCh)

	r.applyConfig(ctx, r.cfg)
	pruneTicker := time.NewTicker(time.Hour)
	defer pruneTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.stopAll()
			return nil
		case <-reloadCh:
			cfg, err := config.LoadFile(r.configPath)
			if err != nil {
				fmt.Fprintf(r.errOut, "config reload failed: %v\n", err)
				continue
			}
			r.applyConfig(ctx, cfg)
			fmt.Fprintln(r.out, "config reloaded")
		case <-pruneTicker.C:
			r.mu.Lock()
			retention := r.cfg.Defaults.HistoryRetention.Duration
			r.mu.Unlock()
			if err := r.store.PruneProbeResults(ctx, retention, time.Now().UTC()); err != nil {
				fmt.Fprintf(r.errOut, "history prune failed: %v\n", err)
			}
		}
	}
}

func (r *Runner) applyConfig(parent context.Context, cfg config.Config) {
	r.mu.Lock()
	defer r.mu.Unlock()

	desired := map[string]config.MonitorConfig{}
	for _, mon := range cfg.Monitors {
		desired[mon.ID] = mon
	}
	for id, cancel := range r.workers {
		if _, ok := desired[id]; !ok {
			cancel()
			delete(r.workers, id)
		}
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
	r.emailer = alert.NewEmailer(cfg.SMTP)
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
		fmt.Fprintf(r.errOut, "state read failed for %s: %v\n", mon.ID, err)
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
	if err := r.store.SaveProbeAndState(ctx, probeResult, next, incident); err != nil {
		fmt.Fprintf(r.errOut, "state write failed for %s: %v\n", mon.ID, err)
		return
	}
	if incident != nil {
		r.mu.Lock()
		emailer := r.emailer
		r.mu.Unlock()
		if err := emailer.SendIncident(*incident, next); err != nil {
			fmt.Fprintf(r.errOut, "email alert failed for %s: %v\n", mon.ID, err)
		}
	}
}
