package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"upag/internal/state"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

type postgresMigration struct {
	ID  string
	SQL string
}

func OpenPostgres(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres database: %w", err)
	}
	store := &PostgresStore{pool: pool}
	if err := store.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

func (s *PostgresStore) migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		id TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := map[string]bool{}
	rows, err := s.pool.Query(ctx, `SELECT id FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("list schema migrations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		applied[id] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, migration := range postgresMigrations() {
		if applied[migration.ID] {
			continue
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", migration.ID, err)
		}
		if _, err := tx.Exec(ctx, migration.SQL); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", migration.ID, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (id, applied_at) VALUES ($1, $2)`, migration.ID, time.Now().UTC()); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", migration.ID, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", migration.ID, err)
		}
	}
	return nil
}

func postgresMigrations() []postgresMigration {
	return []postgresMigration{
		{
			ID: "0001_current_schema",
			SQL: `
CREATE TABLE IF NOT EXISTS monitor_states (
	tenant_id TEXT NOT NULL DEFAULT 'default',
	monitor_id TEXT NOT NULL,
	name TEXT NOT NULL,
	url TEXT NOT NULL,
	expected_status_code INTEGER NOT NULL,
	status TEXT NOT NULL,
	status_before_maintenance TEXT NOT NULL DEFAULT '',
	consecutive_failures INTEGER NOT NULL,
	last_checked_at TIMESTAMPTZ,
	last_success_at TIMESTAMPTZ,
	last_failure_at TIMESTAMPTZ,
	last_error TEXT NOT NULL,
	last_observed_status_code INTEGER NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (tenant_id, monitor_id)
);
CREATE TABLE IF NOT EXISTS probe_results (
	id BIGSERIAL PRIMARY KEY,
	tenant_id TEXT NOT NULL DEFAULT 'default',
	monitor_id TEXT NOT NULL,
	checked_at TIMESTAMPTZ NOT NULL,
	ok BOOLEAN NOT NULL,
	observed_status_code INTEGER NOT NULL,
	latency_ms BIGINT NOT NULL,
	response_time_ms BIGINT NOT NULL DEFAULT 0,
	attempt_count INTEGER NOT NULL DEFAULT 1,
	error TEXT NOT NULL,
	maintenance_window_id BIGINT,
	observer_suppressed BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_probe_results_monitor_checked ON probe_results (tenant_id, monitor_id, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_probe_results_checked ON probe_results (tenant_id, checked_at);
CREATE TABLE IF NOT EXISTS incidents (
	id BIGSERIAL PRIMARY KEY,
	tenant_id TEXT NOT NULL DEFAULT 'default',
	monitor_id TEXT NOT NULL,
	name TEXT NOT NULL,
	transition TEXT NOT NULL,
	observed_at TIMESTAMPTZ NOT NULL,
	error TEXT NOT NULL,
	status_code INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_incidents_observed ON incidents (tenant_id, monitor_id, observed_at DESC);
CREATE TABLE IF NOT EXISTS alert_notifications (
	id BIGSERIAL PRIMARY KEY,
	tenant_id TEXT NOT NULL DEFAULT 'default',
	incident_id BIGINT NOT NULL REFERENCES incidents(id),
	monitor_id TEXT NOT NULL,
	provider TEXT NOT NULL,
	attempted_at TIMESTAMPTZ NOT NULL,
	attempt_number INTEGER NOT NULL DEFAULT 1,
	success BOOLEAN NOT NULL,
	error TEXT NOT NULL,
	next_retry_at TIMESTAMPTZ,
	retry_exhausted BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_alert_notifications_incident ON alert_notifications (tenant_id, incident_id);
CREATE INDEX IF NOT EXISTS idx_alert_notifications_monitor_attempted ON alert_notifications (tenant_id, monitor_id, attempted_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_notifications_attempted ON alert_notifications (tenant_id, attempted_at DESC);
CREATE TABLE IF NOT EXISTS maintenance_windows (
	id BIGSERIAL PRIMARY KEY,
	tenant_id TEXT NOT NULL DEFAULT 'default',
	monitor_id TEXT NOT NULL,
	starts_at TIMESTAMPTZ NOT NULL,
	ends_at TIMESTAMPTZ NOT NULL,
	reason TEXT NOT NULL,
	created_by TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL,
	cancelled_at TIMESTAMPTZ,
	cancelled_by TEXT NOT NULL DEFAULT '',
	cancellation_reason TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_maintenance_windows_monitor_time ON maintenance_windows (tenant_id, monitor_id, starts_at, ends_at);
CREATE TABLE IF NOT EXISTS observer_state (
	tenant_id TEXT NOT NULL DEFAULT 'default',
	id INTEGER NOT NULL CHECK (id = 1),
	status TEXT NOT NULL,
	consecutive_failures INTEGER NOT NULL,
	consecutive_successes INTEGER NOT NULL,
	last_checked_at TIMESTAMPTZ,
	last_success_at TIMESTAMPTZ,
	last_failure_at TIMESTAMPTZ,
	last_error TEXT NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (tenant_id)
);
CREATE TABLE IF NOT EXISTS observer_sentinel_results (
	tenant_id TEXT NOT NULL DEFAULT 'default',
	sentinel_id TEXT NOT NULL,
	name TEXT NOT NULL,
	url TEXT NOT NULL,
	expected_status_code INTEGER NOT NULL,
	ok BOOLEAN NOT NULL,
	observed_status_code INTEGER NOT NULL,
	latency_ms BIGINT NOT NULL,
	error TEXT NOT NULL,
	checked_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (tenant_id, sentinel_id)
);
CREATE TABLE IF NOT EXISTS probe_minute_rollups (
	tenant_id TEXT NOT NULL DEFAULT 'default',
	monitor_id TEXT NOT NULL,
	bucket_start TIMESTAMPTZ NOT NULL,
	total_checks INTEGER NOT NULL,
	successful_checks INTEGER NOT NULL,
	maintenance_checks INTEGER NOT NULL,
	maintenance_failed_checks INTEGER NOT NULL,
	observer_suppressed_checks INTEGER NOT NULL DEFAULT 0,
	first_reportable_at TIMESTAMPTZ,
	last_reportable_at TIMESTAMPTZ,
	PRIMARY KEY (tenant_id, monitor_id, bucket_start)
);
CREATE TABLE IF NOT EXISTS probe_hourly_rollups (LIKE probe_minute_rollups INCLUDING ALL);
CREATE TABLE IF NOT EXISTS probe_daily_rollups (LIKE probe_minute_rollups INCLUDING ALL);
CREATE TABLE IF NOT EXISTS probe_outcome_runs (
	id BIGSERIAL PRIMARY KEY,
	tenant_id TEXT NOT NULL DEFAULT 'default',
	monitor_id TEXT NOT NULL,
	started_at TIMESTAMPTZ NOT NULL,
	ended_at TIMESTAMPTZ NOT NULL,
	ok BOOLEAN NOT NULL,
	probe_count INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_probe_outcome_runs_monitor_started ON probe_outcome_runs (tenant_id, monitor_id, started_at);
CREATE TABLE IF NOT EXISTS monitor_status_intervals (
	id BIGSERIAL PRIMARY KEY,
	tenant_id TEXT NOT NULL DEFAULT 'default',
	monitor_id TEXT NOT NULL,
	status TEXT NOT NULL,
	started_at TIMESTAMPTZ NOT NULL,
	ended_at TIMESTAMPTZ,
	downtime BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_monitor_status_intervals_monitor_started
	ON monitor_status_intervals (tenant_id, monitor_id, started_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_monitor_status_intervals_open
	ON monitor_status_intervals (tenant_id, monitor_id)
	WHERE ended_at IS NULL;
CREATE TABLE IF NOT EXISTS monitor_status_interval_backfills (
	tenant_id TEXT PRIMARY KEY,
	backfilled_at TIMESTAMPTZ NOT NULL,
	failure_threshold INTEGER NOT NULL
);
`,
		},
		{
			ID: "0002_query_path_indexes",
			SQL: `
CREATE INDEX IF NOT EXISTS idx_incidents_monitor_observed ON incidents (tenant_id, monitor_id, observed_at);
CREATE INDEX IF NOT EXISTS idx_alert_notifications_incident_provider_id ON alert_notifications (tenant_id, incident_id, provider, id);
CREATE INDEX IF NOT EXISTS idx_alert_notifications_due_retries ON alert_notifications (tenant_id, next_retry_at, id)
	WHERE success = FALSE
		AND retry_exhausted = FALSE
		AND next_retry_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_probe_minute_rollups_bucket_start ON probe_minute_rollups (tenant_id, bucket_start);
CREATE INDEX IF NOT EXISTS idx_probe_hourly_rollups_bucket_start ON probe_hourly_rollups (tenant_id, bucket_start);
CREATE INDEX IF NOT EXISTS idx_probe_daily_rollups_bucket_start ON probe_daily_rollups (tenant_id, bucket_start);
CREATE INDEX IF NOT EXISTS idx_probe_outcome_runs_ended ON probe_outcome_runs (tenant_id, ended_at);
CREATE INDEX IF NOT EXISTS idx_observer_sentinel_results_tenant_sentinel ON observer_sentinel_results (tenant_id, sentinel_id);
CREATE INDEX IF NOT EXISTS idx_maintenance_windows_tenant_monitor_time ON maintenance_windows (tenant_id, monitor_id, starts_at, ends_at);
CREATE INDEX IF NOT EXISTS idx_maintenance_windows_tenant_id ON maintenance_windows (tenant_id);
CREATE INDEX IF NOT EXISTS idx_monitor_states_tenant_id_monitor ON monitor_states (tenant_id, monitor_id);
CREATE INDEX IF NOT EXISTS idx_probe_results_tenant_monitor_checked ON probe_results (tenant_id, monitor_id, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_incidents_tenant_id ON incidents (tenant_id);
`,
		},
		{
			ID: "0003_tenant_isolation",
			SQL: `
ALTER TABLE monitor_states ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
UPDATE monitor_states SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
ALTER TABLE monitor_states ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE monitor_states ALTER COLUMN tenant_id SET DEFAULT 'default';
ALTER TABLE monitor_states DROP CONSTRAINT IF EXISTS monitor_states_pkey;
ALTER TABLE monitor_states ADD CONSTRAINT monitor_states_pkey PRIMARY KEY (tenant_id, monitor_id);

ALTER TABLE probe_results ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
UPDATE probe_results SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
ALTER TABLE probe_results ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE probe_results ALTER COLUMN tenant_id SET DEFAULT 'default';

ALTER TABLE incidents ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
UPDATE incidents SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
ALTER TABLE incidents ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE incidents ALTER COLUMN tenant_id SET DEFAULT 'default';

ALTER TABLE alert_notifications ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
UPDATE alert_notifications SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
ALTER TABLE alert_notifications ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE alert_notifications ALTER COLUMN tenant_id SET DEFAULT 'default';

ALTER TABLE maintenance_windows ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
UPDATE maintenance_windows SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
ALTER TABLE maintenance_windows ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE maintenance_windows ALTER COLUMN tenant_id SET DEFAULT 'default';

ALTER TABLE observer_state ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
UPDATE observer_state SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
ALTER TABLE observer_state ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE observer_state ALTER COLUMN tenant_id SET DEFAULT 'default';
ALTER TABLE observer_state DROP CONSTRAINT IF EXISTS observer_state_pkey;
ALTER TABLE observer_state ADD CONSTRAINT observer_state_pkey PRIMARY KEY (tenant_id);

ALTER TABLE observer_sentinel_results ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
UPDATE observer_sentinel_results SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
ALTER TABLE observer_sentinel_results ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE observer_sentinel_results ALTER COLUMN tenant_id SET DEFAULT 'default';
ALTER TABLE observer_sentinel_results DROP CONSTRAINT IF EXISTS observer_sentinel_results_pkey;
ALTER TABLE observer_sentinel_results ADD CONSTRAINT observer_sentinel_results_pkey PRIMARY KEY (tenant_id, sentinel_id);

ALTER TABLE probe_minute_rollups ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
UPDATE probe_minute_rollups SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
ALTER TABLE probe_minute_rollups ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE probe_minute_rollups ALTER COLUMN tenant_id SET DEFAULT 'default';
ALTER TABLE probe_minute_rollups DROP CONSTRAINT IF EXISTS probe_minute_rollups_pkey;
ALTER TABLE probe_minute_rollups ADD CONSTRAINT probe_minute_rollups_pkey PRIMARY KEY (tenant_id, monitor_id, bucket_start);

ALTER TABLE probe_hourly_rollups ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
UPDATE probe_hourly_rollups SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
ALTER TABLE probe_hourly_rollups ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE probe_hourly_rollups ALTER COLUMN tenant_id SET DEFAULT 'default';
ALTER TABLE probe_hourly_rollups DROP CONSTRAINT IF EXISTS probe_hourly_rollups_pkey;
ALTER TABLE probe_hourly_rollups ADD CONSTRAINT probe_hourly_rollups_pkey PRIMARY KEY (tenant_id, monitor_id, bucket_start);

ALTER TABLE probe_daily_rollups ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
UPDATE probe_daily_rollups SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
ALTER TABLE probe_daily_rollups ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE probe_daily_rollups ALTER COLUMN tenant_id SET DEFAULT 'default';
ALTER TABLE probe_daily_rollups DROP CONSTRAINT IF EXISTS probe_daily_rollups_pkey;
ALTER TABLE probe_daily_rollups ADD CONSTRAINT probe_daily_rollups_pkey PRIMARY KEY (tenant_id, monitor_id, bucket_start);

ALTER TABLE probe_outcome_runs ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
UPDATE probe_outcome_runs SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
ALTER TABLE probe_outcome_runs ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE probe_outcome_runs ALTER COLUMN tenant_id SET DEFAULT 'default';

DROP INDEX IF EXISTS idx_probe_results_monitor_checked;
DROP INDEX IF EXISTS idx_probe_results_checked;
CREATE INDEX IF NOT EXISTS idx_probe_results_monitor_checked ON probe_results (tenant_id, monitor_id, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_probe_results_checked ON probe_results (tenant_id, checked_at);

DROP INDEX IF EXISTS idx_incidents_observed;
DROP INDEX IF EXISTS idx_incidents_monitor_observed;
CREATE INDEX IF NOT EXISTS idx_incidents_monitor_observed ON incidents (tenant_id, monitor_id, observed_at DESC);

DROP INDEX IF EXISTS idx_alert_notifications_incident;
DROP INDEX IF EXISTS idx_alert_notifications_monitor_attempted;
DROP INDEX IF EXISTS idx_alert_notifications_attempted;
DROP INDEX IF EXISTS idx_alert_notifications_incident_provider_id;
CREATE INDEX IF NOT EXISTS idx_alert_notifications_incident_provider_id ON alert_notifications (tenant_id, incident_id, provider, id);
CREATE INDEX IF NOT EXISTS idx_alert_notifications_due_retries ON alert_notifications (tenant_id, next_retry_at, id)
	WHERE success = FALSE
		AND retry_exhausted = FALSE
		AND next_retry_at IS NOT NULL;

DROP INDEX IF EXISTS idx_maintenance_windows_monitor_time;
CREATE INDEX IF NOT EXISTS idx_maintenance_windows_monitor_time ON maintenance_windows (tenant_id, monitor_id, starts_at, ends_at);

DROP INDEX IF EXISTS idx_probe_minute_rollups_bucket_start;
DROP INDEX IF EXISTS idx_probe_hourly_rollups_bucket_start;
DROP INDEX IF EXISTS idx_probe_daily_rollups_bucket_start;
CREATE INDEX IF NOT EXISTS idx_probe_minute_rollups_bucket_start ON probe_minute_rollups (tenant_id, bucket_start);
CREATE INDEX IF NOT EXISTS idx_probe_hourly_rollups_bucket_start ON probe_hourly_rollups (tenant_id, bucket_start);
CREATE INDEX IF NOT EXISTS idx_probe_daily_rollups_bucket_start ON probe_daily_rollups (tenant_id, bucket_start);

DROP INDEX IF EXISTS idx_probe_outcome_runs_monitor_started;
DROP INDEX IF EXISTS idx_probe_outcome_runs_ended;
CREATE INDEX IF NOT EXISTS idx_probe_outcome_runs_monitor_started ON probe_outcome_runs (tenant_id, monitor_id, started_at);
CREATE INDEX IF NOT EXISTS idx_probe_outcome_runs_ended ON probe_outcome_runs (tenant_id, ended_at);

DROP INDEX IF EXISTS idx_observer_sentinel_results_tenant_sentinel;
CREATE INDEX IF NOT EXISTS idx_observer_sentinel_results_tenant_sentinel ON observer_sentinel_results (tenant_id, sentinel_id);

DROP INDEX IF EXISTS idx_maintenance_windows_tenant_monitor_time;
DROP INDEX IF EXISTS idx_maintenance_windows_tenant_id;
DROP INDEX IF EXISTS idx_incidents_tenant_id;
CREATE INDEX IF NOT EXISTS idx_maintenance_windows_tenant_monitor_time ON maintenance_windows (tenant_id, monitor_id, starts_at, ends_at);
CREATE INDEX IF NOT EXISTS idx_maintenance_windows_tenant_id ON maintenance_windows (tenant_id);
CREATE INDEX IF NOT EXISTS idx_incidents_tenant_id ON incidents (tenant_id);
`,
		},
		{
			ID: "0004_observer_sentinel_events",
			SQL: `
CREATE TABLE IF NOT EXISTS observer_sentinel_events (
	id BIGSERIAL PRIMARY KEY,
	tenant_id TEXT NOT NULL DEFAULT 'default',
	sentinel_id TEXT NOT NULL,
	name TEXT NOT NULL,
	url TEXT NOT NULL,
	expected_status_code INTEGER NOT NULL,
	ok BOOLEAN NOT NULL,
	observed_status_code INTEGER NOT NULL,
	latency_ms BIGINT NOT NULL,
	error TEXT NOT NULL,
	checked_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_observer_sentinel_events_checked
	ON observer_sentinel_events (tenant_id, checked_at DESC);
`,
		},
		{
			ID: "0005_monitor_status_intervals",
			SQL: `
CREATE TABLE IF NOT EXISTS monitor_status_intervals (
	id BIGSERIAL PRIMARY KEY,
	tenant_id TEXT NOT NULL DEFAULT 'default',
	monitor_id TEXT NOT NULL,
	status TEXT NOT NULL,
	started_at TIMESTAMPTZ NOT NULL,
	ended_at TIMESTAMPTZ,
	downtime BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_monitor_status_intervals_monitor_started
	ON monitor_status_intervals (tenant_id, monitor_id, started_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_monitor_status_intervals_open
	ON monitor_status_intervals (tenant_id, monitor_id)
	WHERE ended_at IS NULL;
CREATE TABLE IF NOT EXISTS monitor_status_interval_backfills (
	tenant_id TEXT PRIMARY KEY,
	backfilled_at TIMESTAMPTZ NOT NULL,
	failure_threshold INTEGER NOT NULL
);
`,
		},
		{
			ID:  "0006_maintenance_status_intervals",
			SQL: `DELETE FROM monitor_status_interval_backfills;`,
		},
	}
}

func (s *PostgresStore) GetState(ctx context.Context, monitorID string) (MonitorState, bool, error) {
	tenantID := TenantFromContext(ctx)
	row := s.pool.QueryRow(ctx, `SELECT
		monitor_id, name, url, expected_status_code, status, status_before_maintenance, consecutive_failures,
		last_checked_at, last_success_at, last_failure_at, last_error,
		last_observed_status_code, updated_at
		FROM monitor_states WHERE tenant_id = $1 AND monitor_id = $2`, tenantID, monitorID)
	state, err := scanPostgresState(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return MonitorState{}, false, nil
	}
	if err != nil {
		return MonitorState{}, false, err
	}
	return state, true, nil
}

func (s *PostgresStore) SaveProbeAndState(ctx context.Context, result ProbeResult, next MonitorState, incident *Incident) (int64, error) {
	tenantID := TenantFromContext(ctx)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	attemptCount := result.AttemptCount
	if attemptCount == 0 {
		attemptCount = 1
	}
	if _, err := tx.Exec(ctx, `INSERT INTO probe_results
		(tenant_id, monitor_id, checked_at, ok, observed_status_code, latency_ms, response_time_ms, attempt_count, error, maintenance_window_id, observer_suppressed)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		tenantID, result.MonitorID, postgresNullableTime(result.CheckedAt), result.OK, result.ObservedStatusCode, result.LatencyMS, result.ResponseTimeMS, attemptCount, result.Error, nullableInt64(result.MaintenanceWindowID), result.ObserverSuppressed); err != nil {
		return 0, fmt.Errorf("insert probe result: %w", err)
	}

	if _, err := tx.Exec(ctx, `INSERT INTO monitor_states
		(tenant_id, monitor_id, name, url, expected_status_code, status, status_before_maintenance, consecutive_failures,
		last_checked_at, last_success_at, last_failure_at, last_error,
		last_observed_status_code, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT(tenant_id, monitor_id) DO UPDATE SET
			name = EXCLUDED.name,
			url = EXCLUDED.url,
			expected_status_code = EXCLUDED.expected_status_code,
			status = EXCLUDED.status,
			status_before_maintenance = EXCLUDED.status_before_maintenance,
			consecutive_failures = EXCLUDED.consecutive_failures,
			last_checked_at = EXCLUDED.last_checked_at,
			last_success_at = EXCLUDED.last_success_at,
			last_failure_at = EXCLUDED.last_failure_at,
			last_error = EXCLUDED.last_error,
			last_observed_status_code = EXCLUDED.last_observed_status_code,
			updated_at = EXCLUDED.updated_at`,
		tenantID, next.MonitorID, next.Name, next.URL, next.ExpectedStatusCode, next.Status, next.StatusBeforeMaintenance, next.ConsecutiveFailures,
		postgresNullableTime(next.LastCheckedAt), postgresNullableTime(next.LastSuccessAt), postgresNullableTime(next.LastFailureAt), next.LastError,
		next.LastObservedStatusCode, next.UpdatedAt.UTC()); err != nil {
		return 0, fmt.Errorf("save monitor state: %w", err)
	}
	if next.Status != state.Maintenance {
		if err := postgresSaveStatusInterval(ctx, tx, tenantID, next.MonitorID, next.Status, next.UpdatedAt); err != nil {
			return 0, fmt.Errorf("save monitor status interval: %w", err)
		}
	}

	var incidentID int64
	if incident != nil {
		if err := tx.QueryRow(ctx, `INSERT INTO incidents
			(tenant_id, monitor_id, name, transition, observed_at, error, status_code)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			RETURNING id`,
			tenantID, incident.MonitorID, incident.Name, incident.Transition, incident.ObservedAt.UTC(), incident.Error, incident.StatusCode).Scan(&incidentID); err != nil {
			return 0, fmt.Errorf("insert incident: %w", err)
		}
		incident.ID = incidentID
	}

	return incidentID, tx.Commit(ctx)
}

func postgresSaveStatusInterval(ctx context.Context, tx pgx.Tx, tenantID string, monitorID string, status string, observedAt time.Time) error {
	observedAt = observedAt.UTC()
	var id int64
	var previousStatus string
	err := tx.QueryRow(ctx, `SELECT id, status
		FROM monitor_status_intervals
		WHERE tenant_id = $1 AND monitor_id = $2 AND ended_at IS NULL
		ORDER BY started_at DESC, id DESC LIMIT 1`, tenantID, monitorID).Scan(&id, &previousStatus)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil && previousStatus == status {
		return nil
	}
	downtime := statusCountsAsDowntime(status)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(ctx, `INSERT INTO monitor_status_intervals
			(tenant_id, monitor_id, status, started_at, ended_at, downtime)
			VALUES ($1, $2, $3, $4, NULL, $5)`, tenantID, monitorID, status, observedAt, downtime)
		return err
	}
	previousDowntime := statusCountsAsDowntime(previousStatus) || (previousStatus == state.Failing && status == state.Down)
	if _, err := tx.Exec(ctx, `UPDATE monitor_status_intervals
		SET ended_at = $1, downtime = $2
		WHERE tenant_id = $3 AND id = $4`, observedAt, previousDowntime, tenantID, id); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO monitor_status_intervals
		(tenant_id, monitor_id, status, started_at, ended_at, downtime)
		VALUES ($1, $2, $3, $4, NULL, $5)`, tenantID, monitorID, status, observedAt, downtime)
	return err
}

func (s *PostgresStore) SaveProbeResult(ctx context.Context, result ProbeResult) error {
	tenantID := TenantFromContext(ctx)
	attemptCount := result.AttemptCount
	if attemptCount == 0 {
		attemptCount = 1
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO probe_results
		(tenant_id, monitor_id, checked_at, ok, observed_status_code, latency_ms, response_time_ms, attempt_count, error, maintenance_window_id, observer_suppressed)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		tenantID, result.MonitorID, result.CheckedAt.UTC(), result.OK, result.ObservedStatusCode, result.LatencyMS, result.ResponseTimeMS, attemptCount, result.Error, nullableInt64(result.MaintenanceWindowID), result.ObserverSuppressed)
	if err != nil {
		return fmt.Errorf("insert probe result: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetObserverState(ctx context.Context) (ObserverState, bool, error) {
	tenantID := TenantFromContext(ctx)
	row := s.pool.QueryRow(ctx, `SELECT
		status, consecutive_failures, consecutive_successes, last_checked_at,
		last_success_at, last_failure_at, last_error, updated_at
		FROM observer_state WHERE tenant_id = $1 AND id = 1`, tenantID)
	state, err := scanPostgresObserverState(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return ObserverState{}, false, nil
	}
	if err != nil {
		return ObserverState{}, false, err
	}
	return state, true, nil
}

func (s *PostgresStore) ListObserverSentinelResults(ctx context.Context) ([]ObserverSentinelResult, error) {
	tenantID := TenantFromContext(ctx)
	rows, err := s.pool.Query(ctx, `SELECT
		sentinel_id, name, url, expected_status_code, ok, observed_status_code,
		latency_ms, error, checked_at
		FROM observer_sentinel_results WHERE tenant_id = $1 ORDER BY sentinel_id ASC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []ObserverSentinelResult
	for rows.Next() {
		result, err := scanPostgresObserverSentinelResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func (s *PostgresStore) SaveObserverCheck(ctx context.Context, state ObserverState, results []ObserverSentinelResult, incident *Incident) (int64, error) {
	tenantID := TenantFromContext(ctx)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `INSERT INTO observer_state
		(tenant_id, id, status, consecutive_failures, consecutive_successes, last_checked_at,
		 last_success_at, last_failure_at, last_error, updated_at)
		VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT(tenant_id) DO UPDATE SET
			status = EXCLUDED.status,
			consecutive_failures = EXCLUDED.consecutive_failures,
			consecutive_successes = EXCLUDED.consecutive_successes,
			last_checked_at = EXCLUDED.last_checked_at,
			last_success_at = EXCLUDED.last_success_at,
			last_failure_at = EXCLUDED.last_failure_at,
			last_error = EXCLUDED.last_error,
			updated_at = EXCLUDED.updated_at`,
		tenantID, state.Status, state.ConsecutiveFailures, state.ConsecutiveSuccesses, postgresNullableTime(state.LastCheckedAt),
		postgresNullableTime(state.LastSuccessAt), postgresNullableTime(state.LastFailureAt), state.LastError, state.UpdatedAt.UTC()); err != nil {
		return 0, fmt.Errorf("save observer state: %w", err)
	}
	for _, result := range results {
		if _, err := tx.Exec(ctx, `INSERT INTO observer_sentinel_results
			(tenant_id, sentinel_id, name, url, expected_status_code, ok, observed_status_code, latency_ms, error, checked_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT(tenant_id, sentinel_id) DO UPDATE SET
				name = EXCLUDED.name,
				url = EXCLUDED.url,
				expected_status_code = EXCLUDED.expected_status_code,
				ok = EXCLUDED.ok,
				observed_status_code = EXCLUDED.observed_status_code,
				latency_ms = EXCLUDED.latency_ms,
				error = EXCLUDED.error,
				checked_at = EXCLUDED.checked_at`,
			tenantID, result.SentinelID, result.Name, result.URL, result.ExpectedStatusCode, result.OK,
			result.ObservedStatusCode, result.LatencyMS, result.Error, result.CheckedAt.UTC()); err != nil {
			return 0, fmt.Errorf("save observer sentinel result: %w", err)
		}
		if !result.OK {
			if _, err := tx.Exec(ctx, `INSERT INTO observer_sentinel_events
				(tenant_id, sentinel_id, name, url, expected_status_code, ok, observed_status_code, latency_ms, error, checked_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
				tenantID, result.SentinelID, result.Name, result.URL, result.ExpectedStatusCode, result.OK,
				result.ObservedStatusCode, result.LatencyMS, result.Error, result.CheckedAt.UTC()); err != nil {
				return 0, fmt.Errorf("save observer sentinel event: %w", err)
			}
		}
	}

	var incidentID int64
	if incident != nil {
		if err := tx.QueryRow(ctx, `INSERT INTO incidents
			(tenant_id, monitor_id, name, transition, observed_at, error, status_code)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			RETURNING id`,
			tenantID, incident.MonitorID, incident.Name, incident.Transition, incident.ObservedAt.UTC(), incident.Error, incident.StatusCode).Scan(&incidentID); err != nil {
			return 0, fmt.Errorf("insert incident: %w", err)
		}
		incident.ID = incidentID
	}
	return incidentID, tx.Commit(ctx)
}

func (s *PostgresStore) SaveAlertNotifications(ctx context.Context, notifications []AlertNotification) error {
	if len(notifications) == 0 {
		return nil
	}
	tenantID := TenantFromContext(ctx)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, notification := range notifications {
		attemptNumber := notification.AttemptNumber
		if attemptNumber == 0 {
			attemptNumber = 1
		}
		if _, err := tx.Exec(ctx, `INSERT INTO alert_notifications
			(tenant_id, incident_id, monitor_id, provider, attempted_at, attempt_number, success, error, next_retry_at, retry_exhausted)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			tenantID, notification.IncidentID, notification.MonitorID, notification.Provider, notification.AttemptedAt.UTC(),
			attemptNumber, notification.Success, notification.Error, postgresNullableTime(notification.NextRetryAt),
			notification.RetryExhausted); err != nil {
			return fmt.Errorf("insert alert notification: %w", err)
		}
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) ListStates(ctx context.Context) ([]MonitorState, error) {
	tenantID := TenantFromContext(ctx)
	rows, err := s.pool.Query(ctx, `SELECT
		monitor_id, name, url, expected_status_code, status, status_before_maintenance, consecutive_failures,
		last_checked_at, last_success_at, last_failure_at, last_error,
		last_observed_status_code, updated_at
		FROM monitor_states WHERE tenant_id = $1 ORDER BY last_checked_at DESC, monitor_id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var states []MonitorState
	for rows.Next() {
		state, err := scanPostgresState(rows)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, rows.Err()
}

func (s *PostgresStore) ListStatusIntervals(ctx context.Context, filter StatusIntervalFilter) ([]StatusInterval, error) {
	tenantID := TenantFromContext(ctx)
	monitorID := strings.TrimSpace(filter.MonitorID)
	query := `SELECT id, monitor_id, status, started_at, ended_at, downtime
		FROM monitor_status_intervals
		WHERE tenant_id = $1`
	args := []any{tenantID}
	if monitorID != "" {
		args = append(args, monitorID)
		query += fmt.Sprintf(` AND monitor_id = $%d`, len(args))
	}
	if !filter.Since.IsZero() {
		args = append(args, filter.Since.UTC())
		query += fmt.Sprintf(` AND (started_at >= $%d OR ended_at IS NULL OR ended_at >= $%d)`, len(args), len(args))
	}
	query += ` ORDER BY started_at ASC, id ASC`
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var intervals []StatusInterval
	for rows.Next() {
		var interval StatusInterval
		var endedAt *time.Time
		if err := rows.Scan(&interval.ID, &interval.MonitorID, &interval.Status, &interval.StartedAt, &endedAt, &interval.Downtime); err != nil {
			return nil, err
		}
		interval.StartedAt = interval.StartedAt.UTC()
		if endedAt != nil {
			interval.EndedAt = endedAt.UTC()
		}
		intervals = append(intervals, interval)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	windows, err := s.ListMaintenanceWindows(ctx, MaintenanceWindowFilter{MonitorID: monitorID})
	if err != nil {
		return nil, err
	}
	return projectStatusIntervals(intervals, windows, filter), nil
}

func (s *PostgresStore) ListUptimeStats(ctx context.Context, now time.Time, thresholds FailureThresholds) (map[string]UptimeStats, error) {
	if err := s.EnsureStatusIntervalsBackfilled(ctx, thresholds); err != nil {
		return nil, err
	}
	stats := map[string]UptimeStats{}
	windows := []struct {
		name   string
		cutoff time.Time
	}{
		{name: "24h", cutoff: now.UTC().Add(-24 * time.Hour)},
		{name: "7d", cutoff: now.UTC().Add(-7 * 24 * time.Hour)},
		{name: "30d", cutoff: now.UTC().Add(-30 * 24 * time.Hour)},
		{name: "retained"},
	}
	for _, window := range windows {
		windowStats, err := s.listUptimeWindowStats(ctx, window.cutoff)
		if err != nil {
			return nil, err
		}
		for monitorID, uptime := range windowStats {
			monitorStats := stats[monitorID]
			switch window.name {
			case "24h":
				monitorStats.TwentyFourHour = uptime
			case "7d":
				monitorStats.SevenDay = uptime
			case "30d":
				monitorStats.ThirtyDay = uptime
			case "retained":
				monitorStats.Retained = uptime
			}
			stats[monitorID] = monitorStats
		}
	}
	if err := s.addStrictAvailability(ctx, now.UTC(), stats); err != nil {
		return nil, err
	}
	return stats, nil
}

func (s *PostgresStore) ListDailyUptimeStats(ctx context.Context, now time.Time, days int, thresholds FailureThresholds) (map[string][]DailyUptimeStats, error) {
	if err := s.EnsureStatusIntervalsBackfilled(ctx, thresholds); err != nil {
		return nil, err
	}
	retained, err := s.listUptimeWindowStats(ctx, time.Time{})
	if err != nil {
		return nil, err
	}
	outages, err := s.listDowntimeIntervals(ctx, now.UTC())
	if err != nil {
		return nil, err
	}
	maintenance, err := s.listUncancelledMaintenanceWindows(ctx)
	if err != nil {
		return nil, err
	}
	states, err := s.ListStates(ctx)
	if err != nil {
		return nil, err
	}
	return dailyUptimeStats(now, days, states, retained, outages, maintenance), nil
}

func (s *PostgresStore) listUptimeWindowStats(ctx context.Context, cutoff time.Time) (map[string]UptimeWindowStats, error) {
	tenantID := TenantFromContext(ctx)
	rawWhere := ""
	rollupWhere := ""
	args := []any{tenantID}
	rawWhere = ` WHERE tenant_id = $1`
	rollupWhere = ` WHERE tenant_id = $1`
	if !cutoff.IsZero() {
		args = append(args, cutoff.UTC())
		rawWhere = ` WHERE tenant_id = $1 AND checked_at >= $2`
		rollupWhere = ` WHERE tenant_id = $1 AND bucket_start >= $2`
	}
	rows, err := s.pool.Query(ctx, `SELECT monitor_id,
			COALESCE(SUM(total_checks), 0)::int,
			COALESCE(SUM(successful_checks), 0)::int,
			COALESCE(SUM(maintenance_checks), 0)::int,
			COALESCE(SUM(maintenance_failed_checks), 0)::int,
			MIN(first_reportable_at),
			MAX(last_reportable_at)
		FROM (
			SELECT monitor_id,
				COALESCE(SUM(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = FALSE THEN 1 ELSE 0 END), 0)::int AS total_checks,
				COALESCE(SUM(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = FALSE AND ok THEN 1 ELSE 0 END), 0)::int AS successful_checks,
				COALESCE(SUM(CASE WHEN maintenance_window_id IS NOT NULL THEN 1 ELSE 0 END), 0)::int AS maintenance_checks,
				COALESCE(SUM(CASE WHEN maintenance_window_id IS NOT NULL AND NOT ok THEN 1 ELSE 0 END), 0)::int AS maintenance_failed_checks,
				MIN(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = FALSE THEN checked_at ELSE NULL END) AS first_reportable_at,
				MAX(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = FALSE THEN checked_at ELSE NULL END) AS last_reportable_at
			FROM probe_results`+rawWhere+`
			GROUP BY monitor_id
			UNION ALL
			SELECT monitor_id, total_checks, successful_checks, maintenance_checks, maintenance_failed_checks, first_reportable_at, last_reportable_at
			FROM probe_minute_rollups`+rollupWhere+`
			UNION ALL
			SELECT monitor_id, total_checks, successful_checks, maintenance_checks, maintenance_failed_checks, first_reportable_at, last_reportable_at
			FROM probe_hourly_rollups`+rollupWhere+`
			UNION ALL
			SELECT monitor_id, total_checks, successful_checks, maintenance_checks, maintenance_failed_checks, first_reportable_at, last_reportable_at
			FROM probe_daily_rollups`+rollupWhere+`
		) uptime
		GROUP BY monitor_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := map[string]UptimeWindowStats{}
	for rows.Next() {
		var monitorID string
		var totalChecks int
		var successfulChecks int
		var maintenanceChecks int
		var maintenanceFailedChecks int
		var startedAt *time.Time
		var endedAt *time.Time
		if err := rows.Scan(&monitorID, &totalChecks, &successfulChecks, &maintenanceChecks, &maintenanceFailedChecks, &startedAt, &endedAt); err != nil {
			return nil, err
		}
		stats[monitorID] = UptimeWindowStats{
			TotalChecks:             totalChecks,
			SuccessfulChecks:        successfulChecks,
			FailedChecks:            totalChecks - successfulChecks,
			MaintenanceChecks:       maintenanceChecks,
			MaintenanceFailedChecks: maintenanceFailedChecks,
			WindowStartedAt:         derefTime(startedAt),
			WindowEndedAt:           derefTime(endedAt),
		}
	}
	return stats, rows.Err()
}

func (s *PostgresStore) addStrictAvailability(ctx context.Context, now time.Time, stats map[string]UptimeStats) error {
	outagesByMonitor, err := s.listDowntimeIntervals(ctx, now)
	if err != nil {
		return err
	}
	maintenance, err := s.listUncancelledMaintenanceWindows(ctx)
	if err != nil {
		return err
	}
	maintenanceByMonitor := map[string][]timeInterval{}
	for _, window := range maintenance {
		maintenanceByMonitor[window.MonitorID] = append(maintenanceByMonitor[window.MonitorID], timeInterval{Start: window.StartsAt, End: window.EndsAt})
	}
	for monitorID, monitorStats := range stats {
		firstProbe := monitorStats.Retained.WindowStartedAt
		if firstProbe.IsZero() {
			continue
		}
		outages := outagesByMonitor[monitorID]
		maintenanceWindows := maintenanceByMonitor[monitorID]
		monitorStats.TwentyFourHour = applyAvailabilityWindow(monitorStats.TwentyFourHour, firstProbe, now.Add(-24*time.Hour), now, outages, maintenanceWindows)
		monitorStats.SevenDay = applyAvailabilityWindow(monitorStats.SevenDay, firstProbe, now.Add(-7*24*time.Hour), now, outages, maintenanceWindows)
		monitorStats.ThirtyDay = applyAvailabilityWindow(monitorStats.ThirtyDay, firstProbe, now.Add(-30*24*time.Hour), now, outages, maintenanceWindows)
		monitorStats.Retained = applyAvailabilityWindow(monitorStats.Retained, firstProbe, time.Time{}, now, outages, maintenanceWindows)
		stats[monitorID] = monitorStats
	}
	return nil
}

func (s *PostgresStore) listDowntimeIntervals(ctx context.Context, now time.Time) (map[string][]timeInterval, error) {
	tenantID := TenantFromContext(ctx)
	rows, err := s.pool.Query(ctx, `SELECT monitor_id, started_at, ended_at
		FROM monitor_status_intervals
		WHERE tenant_id = $1 AND downtime = TRUE
		ORDER BY monitor_id, started_at`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	outages := map[string][]timeInterval{}
	for rows.Next() {
		var monitorID string
		var startedAt time.Time
		var endedAt *time.Time
		if err := rows.Scan(&monitorID, &startedAt, &endedAt); err != nil {
			return nil, err
		}
		end := now
		if endedAt != nil {
			end = endedAt.UTC()
		}
		outages[monitorID] = append(outages[monitorID], timeInterval{
			Start: startedAt.UTC(),
			End:   end,
		})
	}
	return outages, rows.Err()
}

func (s *PostgresStore) EnsureStatusIntervalsBackfilled(ctx context.Context, thresholds FailureThresholds) error {
	tenantID := TenantFromContext(ctx)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Acquire this before the first DELETE takes its MVCC snapshot. Otherwise a
	// concurrent status transition can commit a new open interval after that
	// snapshot, leaving it behind for the rebuild to conflict with.
	if _, err := tx.Exec(ctx, `LOCK TABLE monitor_status_intervals IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return err
	}

	var marker string
	err = tx.QueryRow(ctx, `SELECT tenant_id FROM monitor_status_interval_backfills WHERE tenant_id = $1`, tenantID).Scan(&marker)
	if err == nil {
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM monitor_status_intervals WHERE tenant_id = $1`, tenantID); err != nil {
		return err
	}
	runs, err := postgresListReportableUptimeRunsTx(ctx, tx, tenantID)
	if err != nil {
		return err
	}
	if err := postgresBackfillStatusIntervalsFromRuns(ctx, tx, tenantID, runs, thresholds); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO monitor_status_interval_backfills
		(tenant_id, backfilled_at, failure_threshold)
		VALUES ($1, $2, $3)`, tenantID, time.Now().UTC(), thresholds.ForMonitor("")); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func postgresListReportableUptimeRunsTx(ctx context.Context, tx pgx.Tx, tenantID string) ([]uptimeRun, error) {
	rows, err := tx.Query(ctx, `SELECT monitor_id, started_at, ended_at, ok, probe_count
		FROM probe_outcome_runs
		WHERE tenant_id = $1
		UNION ALL
		SELECT monitor_id, checked_at AS started_at, checked_at AS ended_at, ok, 1 AS probe_count
		FROM probe_results
		WHERE tenant_id = $1
			AND maintenance_window_id IS NULL
			AND observer_suppressed = FALSE
		ORDER BY monitor_id ASC, started_at ASC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []uptimeRun
	for rows.Next() {
		var run uptimeRun
		if err := rows.Scan(&run.MonitorID, &run.StartedAt, &run.EndedAt, &run.OK, &run.ProbeCount); err != nil {
			return nil, err
		}
		run.StartedAt = run.StartedAt.UTC()
		run.EndedAt = run.EndedAt.UTC()
		if len(runs) > 0 {
			last := &runs[len(runs)-1]
			if last.MonitorID == run.MonitorID && last.OK == run.OK {
				last.EndedAt = run.EndedAt
				last.ProbeCount += run.ProbeCount
				continue
			}
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func postgresBackfillStatusIntervalsFromRuns(ctx context.Context, tx pgx.Tx, tenantID string, runs []uptimeRun, thresholds FailureThresholds) error {
	for i, run := range runs {
		failureThreshold := thresholds.ForMonitor(run.MonitorID)
		statusValue := state.Up
		if !run.OK {
			statusValue = state.Failing
			if run.ProbeCount >= failureThreshold {
				statusValue = state.Down
			}
		}
		var endedAt any
		if i+1 < len(runs) && runs[i+1].MonitorID == run.MonitorID {
			endedAt = runs[i+1].StartedAt.UTC()
		}
		if _, err := tx.Exec(ctx, `INSERT INTO monitor_status_intervals
			(tenant_id, monitor_id, status, started_at, ended_at, downtime)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			tenantID, run.MonitorID, statusValue, run.StartedAt.UTC(), endedAt, statusCountsAsDowntime(statusValue)); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) listUncancelledMaintenanceWindows(ctx context.Context) ([]MaintenanceWindow, error) {
	tenantID := TenantFromContext(ctx)
	rows, err := s.pool.Query(ctx, `SELECT
		id, monitor_id, starts_at, ends_at, reason, created_by, created_at,
		cancelled_at, cancelled_by, cancellation_reason
		FROM maintenance_windows
		WHERE tenant_id = $1
			AND cancelled_at IS NULL
		ORDER BY monitor_id ASC, starts_at ASC, id ASC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var windows []MaintenanceWindow
	for rows.Next() {
		window, err := scanPostgresMaintenanceWindow(rows)
		if err != nil {
			return nil, err
		}
		windows = append(windows, window)
	}
	return windows, rows.Err()
}

func (s *PostgresStore) ListIncidents(ctx context.Context, filter IncidentFilter) ([]Incident, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	tenantID := TenantFromContext(ctx)
	query := `SELECT
		id, monitor_id, name, transition, observed_at, error, status_code
		FROM (
			SELECT id, monitor_id, name, transition, observed_at, error, status_code
			FROM incidents
			WHERE tenant_id = $1
			UNION ALL
			SELECT 0 AS id, probe_results.monitor_id, COALESCE(monitor_states.name, '') AS name,
				'FAILURE' AS transition, probe_results.checked_at AS observed_at, probe_results.error,
				probe_results.observed_status_code AS status_code
			FROM probe_results
			LEFT JOIN monitor_states ON monitor_states.tenant_id = probe_results.tenant_id
				AND monitor_states.monitor_id = probe_results.monitor_id
			WHERE probe_results.tenant_id = $1
				AND probe_results.ok = FALSE
				AND probe_results.maintenance_window_id IS NULL
				AND probe_results.observer_suppressed = FALSE
				AND NOT EXISTS (
					SELECT 1 FROM incidents
					WHERE tenant_id = $1
						AND incidents.monitor_id = probe_results.monitor_id
						AND incidents.observed_at = probe_results.checked_at
				)
		) all_incidents`
	args := []any{tenantID}
	if !filter.Since.IsZero() {
		args = append(args, filter.Since.UTC())
		query += fmt.Sprintf(` WHERE observed_at >= $%d`, len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(` ORDER BY observed_at DESC, id DESC LIMIT $%d`, len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var incidents []Incident
	for rows.Next() {
		var incident Incident
		if err := rows.Scan(&incident.ID, &incident.MonitorID, &incident.Name, &incident.Transition, &incident.ObservedAt, &incident.Error, &incident.StatusCode); err != nil {
			return nil, err
		}
		incident.ObservedAt = incident.ObservedAt.UTC()
		incidents = append(incidents, incident)
	}
	return incidents, rows.Err()
}

func (s *PostgresStore) ListLatestDownIncidentTimes(ctx context.Context) (map[string]time.Time, error) {
	tenantID := TenantFromContext(ctx)
	rows, err := s.pool.Query(ctx, `SELECT monitor_id, MAX(observed_at)
		FROM incidents
		WHERE tenant_id = $1 AND transition = $2
		GROUP BY monitor_id`, tenantID, state.Down)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	latest := map[string]time.Time{}
	for rows.Next() {
		var monitorID string
		var observedAt time.Time
		if err := rows.Scan(&monitorID, &observedAt); err != nil {
			return nil, err
		}
		latest[monitorID] = observedAt.UTC()
	}
	return latest, rows.Err()
}

func (s *PostgresStore) ListFailedProbeResults(ctx context.Context, filter ProbeResultFilter) ([]ProbeResult, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	tenantID := TenantFromContext(ctx)
	query := `SELECT
		monitor_id, checked_at, ok, observed_status_code, latency_ms, response_time_ms,
		attempt_count, error, maintenance_window_id, observer_suppressed
		FROM probe_results WHERE tenant_id = $1 AND ok = FALSE`
	args := []any{tenantID}
	if !filter.Since.IsZero() {
		args = append(args, filter.Since.UTC())
		query += fmt.Sprintf(` AND checked_at >= $%d`, len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(` ORDER BY checked_at DESC LIMIT $%d`, len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []ProbeResult
	for rows.Next() {
		result, err := scanPostgresFailedProbeResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func (s *PostgresStore) ListObserverSentinelEvents(ctx context.Context, filter ObserverSentinelEventFilter) ([]ObserverSentinelResult, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	tenantID := TenantFromContext(ctx)
	query := `SELECT
		sentinel_id, name, url, expected_status_code, ok, observed_status_code,
		latency_ms, error, checked_at
		FROM observer_sentinel_events WHERE tenant_id = $1`
	args := []any{tenantID}
	if !filter.Since.IsZero() {
		args = append(args, filter.Since.UTC())
		query += fmt.Sprintf(` AND checked_at >= $%d`, len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(` ORDER BY checked_at DESC LIMIT $%d`, len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []ObserverSentinelResult
	for rows.Next() {
		result, err := scanPostgresObserverSentinelResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func (s *PostgresStore) ListAlertNotifications(ctx context.Context, limit int) ([]AlertNotification, error) {
	if limit <= 0 {
		limit = 50
	}
	tenantID := TenantFromContext(ctx)
	rows, err := s.pool.Query(ctx, `SELECT
		id, incident_id, monitor_id, provider, attempted_at, attempt_number, success, error, next_retry_at, retry_exhausted
		FROM alert_notifications
		WHERE tenant_id = $1
		ORDER BY attempted_at DESC, id DESC LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var notifications []AlertNotification
	for rows.Next() {
		notification, err := scanPostgresAlertNotification(rows)
		if err != nil {
			return nil, err
		}
		notifications = append(notifications, notification)
	}
	return notifications, rows.Err()
}

func (s *PostgresStore) ListActionableAlertDeliveryFailures(ctx context.Context, limit int) ([]AlertNotification, error) {
	if limit <= 0 {
		limit = 50
	}
	tenantID := TenantFromContext(ctx)
	rows, err := s.pool.Query(ctx, `SELECT
		n.id, n.incident_id, n.monitor_id, n.provider, n.attempted_at, n.attempt_number, n.success, n.error, n.next_retry_at, n.retry_exhausted
		FROM alert_notifications n
		WHERE n.tenant_id = $1
			AND n.success = FALSE
			AND NOT EXISTS (
				SELECT 1 FROM alert_notifications newer
					WHERE newer.tenant_id = n.tenant_id
						AND newer.incident_id = n.incident_id
					AND newer.provider = n.provider
					AND newer.id > n.id
			)
		ORDER BY n.attempted_at DESC, n.id DESC LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var notifications []AlertNotification
	for rows.Next() {
		notification, err := scanPostgresAlertNotification(rows)
		if err != nil {
			return nil, err
		}
		notifications = append(notifications, notification)
	}
	return notifications, rows.Err()
}

func (s *PostgresStore) ListDueAlertNotificationRetries(ctx context.Context, now time.Time, limit int) ([]AlertNotificationRetry, error) {
	if limit <= 0 {
		limit = 50
	}
	tenantID := TenantFromContext(ctx)
	rows, err := s.pool.Query(ctx, `SELECT
		n.id, n.incident_id, n.monitor_id, n.provider, n.attempted_at, n.attempt_number,
		n.success, n.error, n.next_retry_at, n.retry_exhausted,
		i.id, i.monitor_id, i.name, i.transition, i.observed_at, i.error, i.status_code,
		COALESCE(s.monitor_id, i.monitor_id), COALESCE(s.name, i.name), COALESCE(s.url, ''), COALESCE(s.expected_status_code, 0),
		COALESCE(s.status, o.status, ''), COALESCE(s.status_before_maintenance, ''), COALESCE(s.consecutive_failures, o.consecutive_failures, 0),
		COALESCE(s.last_checked_at, o.last_checked_at), COALESCE(s.last_success_at, o.last_success_at),
		COALESCE(s.last_failure_at, o.last_failure_at), COALESCE(s.last_error, o.last_error, ''),
		COALESCE(s.last_observed_status_code, 0), COALESCE(s.updated_at, o.updated_at)
		FROM alert_notifications n
		INNER JOIN incidents i ON i.id = n.incident_id
			AND i.tenant_id = n.tenant_id
		LEFT JOIN monitor_states s ON s.tenant_id = n.tenant_id
			AND s.monitor_id = n.monitor_id
		LEFT JOIN observer_state o ON n.monitor_id = '__observer__'
			AND o.tenant_id = n.tenant_id
		WHERE n.tenant_id = $1
			AND n.success = FALSE
			AND n.retry_exhausted = FALSE
			AND n.next_retry_at IS NOT NULL
			AND n.next_retry_at <= $2
			AND NOT EXISTS (
				SELECT 1 FROM alert_notifications newer
					WHERE newer.tenant_id = n.tenant_id
						AND newer.incident_id = n.incident_id
					AND newer.provider = n.provider
					AND newer.id > n.id
			)
			AND NOT EXISTS (
				SELECT 1 FROM alert_notifications successful
					WHERE successful.tenant_id = n.tenant_id
						AND successful.incident_id = n.incident_id
					AND successful.provider = n.provider
					AND successful.success = TRUE
					AND successful.id > n.id
			)
		ORDER BY n.next_retry_at ASC, n.id ASC LIMIT $3`, tenantID, now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var retries []AlertNotificationRetry
	for rows.Next() {
		var retry AlertNotificationRetry
		var stateLastChecked, stateLastSuccess, stateLastFailure, stateUpdated *time.Time
		if err := rows.Scan(
			&retry.Notification.ID, &retry.Notification.IncidentID, &retry.Notification.MonitorID, &retry.Notification.Provider,
			&retry.Notification.AttemptedAt, &retry.Notification.AttemptNumber, &retry.Notification.Success, &retry.Notification.Error,
			&retry.Notification.NextRetryAt, &retry.Notification.RetryExhausted,
			&retry.Incident.ID, &retry.Incident.MonitorID, &retry.Incident.Name, &retry.Incident.Transition,
			&retry.Incident.ObservedAt, &retry.Incident.Error, &retry.Incident.StatusCode,
			&retry.CurrentState.MonitorID, &retry.CurrentState.Name, &retry.CurrentState.URL, &retry.CurrentState.ExpectedStatusCode,
			&retry.CurrentState.Status, &retry.CurrentState.StatusBeforeMaintenance, &retry.CurrentState.ConsecutiveFailures, &stateLastChecked, &stateLastSuccess,
			&stateLastFailure, &retry.CurrentState.LastError, &retry.CurrentState.LastObservedStatusCode, &stateUpdated,
		); err != nil {
			return nil, err
		}
		retry.Notification.AttemptedAt = retry.Notification.AttemptedAt.UTC()
		retry.Notification.NextRetryAt = retry.Notification.NextRetryAt.UTC()
		retry.Incident.ObservedAt = retry.Incident.ObservedAt.UTC()
		retry.CurrentState.LastCheckedAt = derefTime(stateLastChecked)
		retry.CurrentState.LastSuccessAt = derefTime(stateLastSuccess)
		retry.CurrentState.LastFailureAt = derefTime(stateLastFailure)
		retry.CurrentState.UpdatedAt = derefTime(stateUpdated)
		retries = append(retries, retry)
	}
	return retries, rows.Err()
}

func (s *PostgresStore) AddMaintenanceWindow(ctx context.Context, window MaintenanceWindow) (int64, error) {
	tenantID := TenantFromContext(ctx)
	if !window.EndsAt.After(window.StartsAt) {
		return 0, newMaintenanceError(ErrMaintenanceInvalid, "maintenance end must be after start")
	}
	if strings.TrimSpace(window.Reason) == "" {
		return 0, newMaintenanceError(ErrMaintenanceInvalid, "maintenance reason is required")
	}
	if strings.TrimSpace(window.CreatedBy) == "" {
		return 0, newMaintenanceError(ErrMaintenanceInvalid, "maintenance created_by is required")
	}
	var exists int
	if err := s.pool.QueryRow(ctx, `SELECT 1 FROM monitor_states WHERE tenant_id = $1 AND monitor_id = $2`, tenantID, window.MonitorID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, newMaintenanceError(ErrMaintenanceNotFound, "monitor %q does not exist in monitor_states", window.MonitorID)
		}
		return 0, err
	}
	var overlappingID int64
	err := s.pool.QueryRow(ctx, `SELECT id FROM maintenance_windows
		WHERE tenant_id = $1
			AND monitor_id = $2
			AND cancelled_at IS NULL
			AND starts_at < $3
			AND ends_at > $4
		ORDER BY starts_at ASC LIMIT 1`,
		tenantID, window.MonitorID, window.EndsAt.UTC(), window.StartsAt.UTC()).Scan(&overlappingID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	if overlappingID != 0 {
		return 0, newMaintenanceError(ErrMaintenanceConflict, "maintenance window overlaps existing window %d", overlappingID)
	}
	createdAt := window.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	var id int64
	err = s.pool.QueryRow(ctx, `INSERT INTO maintenance_windows
		(tenant_id, monitor_id, starts_at, ends_at, reason, created_by, created_at, cancelled_at, cancelled_by, cancellation_reason)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id`,
		tenantID, window.MonitorID, window.StartsAt.UTC(), window.EndsAt.UTC(), window.Reason, window.CreatedBy,
		createdAt.UTC(), postgresNullableTime(window.CancelledAt), window.CancelledBy, window.CancellationReason).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert maintenance window: %w", err)
	}
	return id, nil
}

func (s *PostgresStore) CancelMaintenanceWindow(ctx context.Context, id int64, cancelledAt time.Time, cancelledBy string, reason string) error {
	tenantID := TenantFromContext(ctx)
	if strings.TrimSpace(cancelledBy) == "" {
		return newMaintenanceError(ErrMaintenanceInvalid, "maintenance cancelled_by is required")
	}
	if cancelledAt.IsZero() {
		cancelledAt = time.Now().UTC()
	}
	result, err := s.pool.Exec(ctx, `UPDATE maintenance_windows
		SET cancelled_at = $3, cancelled_by = $4, cancellation_reason = $5
		WHERE tenant_id = $1 AND id = $2 AND cancelled_at IS NULL`,
		tenantID, id, cancelledAt.UTC(), cancelledBy, reason)
	if err != nil {
		return fmt.Errorf("cancel maintenance window: %w", err)
	}
	if result.RowsAffected() == 0 {
		var existing int
		if err := s.pool.QueryRow(ctx, `SELECT 1 FROM maintenance_windows WHERE tenant_id = $1 AND id = $2`, tenantID, id).Scan(&existing); errors.Is(err, pgx.ErrNoRows) {
			return newMaintenanceError(ErrMaintenanceNotFound, "maintenance window %d does not exist", id)
		} else if err != nil {
			return err
		}
		return newMaintenanceError(ErrMaintenanceConflict, "maintenance window %d is already cancelled", id)
	}
	return nil
}

func (s *PostgresStore) ActiveMaintenanceWindow(ctx context.Context, monitorID string, at time.Time) (MaintenanceWindow, bool, error) {
	tenantID := TenantFromContext(ctx)
	row := s.pool.QueryRow(ctx, `SELECT
		id, monitor_id, starts_at, ends_at, reason, created_by, created_at,
		cancelled_at, cancelled_by, cancellation_reason
		FROM maintenance_windows
		WHERE tenant_id = $1
			AND monitor_id = $2
			AND cancelled_at IS NULL
			AND starts_at <= $3
			AND ends_at > $3
		ORDER BY starts_at ASC LIMIT 1`, tenantID, monitorID, at.UTC())
	window, err := scanPostgresMaintenanceWindow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return MaintenanceWindow{}, false, nil
	}
	if err != nil {
		return MaintenanceWindow{}, false, err
	}
	return window, true, nil
}

func (s *PostgresStore) ListMaintenanceWindows(ctx context.Context, filter MaintenanceWindowFilter) ([]MaintenanceWindow, error) {
	tenantID := TenantFromContext(ctx)
	query := `SELECT
		id, monitor_id, starts_at, ends_at, reason, created_by, created_at,
		cancelled_at, cancelled_by, cancellation_reason
		FROM maintenance_windows`
	var conditions []string
	var args []any
	args = append(args, tenantID)
	conditions = append(conditions, fmt.Sprintf(`tenant_id = $%d`, len(args)))
	if filter.MonitorID != "" {
		args = append(args, filter.MonitorID)
		conditions = append(conditions, fmt.Sprintf(`monitor_id = $%d`, len(args)))
	}
	if !filter.IncludeAll {
		conditions = append(conditions, `cancelled_at IS NULL`)
		if !filter.Now.IsZero() {
			args = append(args, filter.Now.UTC())
			conditions = append(conditions, fmt.Sprintf(`ends_at > $%d`, len(args)))
		}
	}
	query += ` WHERE ` + strings.Join(conditions, ` AND `)
	query += ` ORDER BY starts_at ASC, id ASC`
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var windows []MaintenanceWindow
	for rows.Next() {
		window, err := scanPostgresMaintenanceWindow(rows)
		if err != nil {
			return nil, err
		}
		windows = append(windows, window)
	}
	return windows, rows.Err()
}

func (s *PostgresStore) DeleteStatesExcept(ctx context.Context, monitorIDs []string) error {
	tenantID := TenantFromContext(ctx)
	if len(monitorIDs) == 0 {
		_, err := s.pool.Exec(ctx, `DELETE FROM monitor_states WHERE tenant_id = $1`, tenantID)
		return err
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM monitor_states WHERE tenant_id = $1 AND NOT (monitor_id = ANY($2))`, tenantID, monitorIDs)
	return err
}

func (s *PostgresStore) PruneProbeResults(ctx context.Context, retention time.Duration, now time.Time) error {
	tenantID := TenantFromContext(ctx)
	cutoff := now.UTC().Add(-retention)
	_, err := s.pool.Exec(ctx, `DELETE FROM probe_results WHERE tenant_id = $1 AND checked_at < $2`, tenantID, cutoff)
	return err
}

func (s *PostgresStore) RollupAndPruneProbeResults(ctx context.Context, policy ProbeRetentionPolicy, now time.Time) error {
	tenantID := TenantFromContext(ctx)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if !policy.ProbeResults.Forever {
		cutoff := now.UTC().Add(-policy.ProbeResults.Duration)
		if err := postgresCompactRawOutcomeRuns(ctx, tx, tenantID, cutoff); err != nil {
			return err
		}
		if err := postgresRollupRawProbeResults(ctx, tx, tenantID, "probe_minute_rollups", postgresMinuteBucketExpression("checked_at"), cutoff); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM probe_results WHERE tenant_id = $1 AND checked_at < $2`, tenantID, cutoff); err != nil {
			return err
		}
	}
	if !policy.ProbeMinuteRollups.Forever {
		cutoff := now.UTC().Add(-policy.ProbeMinuteRollups.Duration)
		if err := postgresRollupStoredProbeRollups(ctx, tx, tenantID, "probe_minute_rollups", "probe_hourly_rollups", postgresHourlyBucketExpression("bucket_start"), cutoff); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM probe_minute_rollups WHERE tenant_id = $1 AND bucket_start < $2`, tenantID, cutoff); err != nil {
			return err
		}
	}
	if !policy.ProbeHourlyRollups.Forever {
		cutoff := now.UTC().Add(-policy.ProbeHourlyRollups.Duration)
		if err := postgresRollupStoredProbeRollups(ctx, tx, tenantID, "probe_hourly_rollups", "probe_daily_rollups", postgresDailyBucketExpression("bucket_start"), cutoff); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM probe_hourly_rollups WHERE tenant_id = $1 AND bucket_start < $2`, tenantID, cutoff); err != nil {
			return err
		}
	}
	if !policy.ProbeDailyRollups.Forever {
		cutoff := now.UTC().Add(-policy.ProbeDailyRollups.Duration)
		if _, err := tx.Exec(ctx, `DELETE FROM probe_daily_rollups WHERE tenant_id = $1 AND bucket_start < $2`, tenantID, cutoff); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM probe_outcome_runs WHERE tenant_id = $1 AND ended_at < $2`, tenantID, cutoff); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func postgresCompactRawOutcomeRuns(ctx context.Context, tx pgx.Tx, tenantID string, cutoff time.Time) error {
	rows, err := tx.Query(ctx, `SELECT monitor_id, checked_at, ok
		FROM probe_results
		WHERE tenant_id = $1
			AND checked_at < $2
			AND maintenance_window_id IS NULL
			AND observer_suppressed = FALSE
		ORDER BY monitor_id ASC, checked_at ASC, id ASC`, tenantID, cutoff)
	if err != nil {
		return err
	}
	type rawRun struct {
		monitorID string
		checkedAt time.Time
		ok        bool
	}
	var rawRuns []rawRun
	for rows.Next() {
		var run rawRun
		if err := rows.Scan(&run.monitorID, &run.checkedAt, &run.ok); err != nil {
			rows.Close()
			return err
		}
		rawRuns = append(rawRuns, run)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, run := range rawRuns {
		if err := postgresAppendOutcomeRun(ctx, tx, tenantID, run.monitorID, run.checkedAt.UTC(), run.ok); err != nil {
			return err
		}
	}
	return nil
}

func postgresAppendOutcomeRun(ctx context.Context, tx pgx.Tx, tenantID string, monitorID string, checkedAt time.Time, ok bool) error {
	var id int64
	var lastOK bool
	err := tx.QueryRow(ctx, `SELECT id, ok
		FROM probe_outcome_runs
		WHERE tenant_id = $1
			AND monitor_id = $2
		ORDER BY started_at DESC, id DESC LIMIT 1`, tenantID, monitorID).Scan(&id, &lastOK)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil && lastOK == ok {
		_, err = tx.Exec(ctx, `UPDATE probe_outcome_runs
			SET ended_at = $1, probe_count = probe_count + 1
			WHERE tenant_id = $2 AND id = $3`, checkedAt.UTC(), tenantID, id)
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO probe_outcome_runs
		(tenant_id, monitor_id, started_at, ended_at, ok, probe_count)
		VALUES ($1, $2, $3, $4, $5, 1)`, tenantID, monitorID, checkedAt, checkedAt, ok)
	return err
}

func postgresRollupRawProbeResults(ctx context.Context, tx pgx.Tx, tenantID string, target string, bucketExpr string, cutoff time.Time) error {
	query := `INSERT INTO ` + target + `
		(tenant_id, monitor_id, bucket_start, total_checks, successful_checks, maintenance_checks, maintenance_failed_checks,
			observer_suppressed_checks, first_reportable_at, last_reportable_at)
		SELECT $1, monitor_id, ` + bucketExpr + ` AS bucket_start,
			COALESCE(SUM(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = FALSE THEN 1 ELSE 0 END), 0)::int,
			COALESCE(SUM(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = FALSE AND ok THEN 1 ELSE 0 END), 0)::int,
			COALESCE(SUM(CASE WHEN maintenance_window_id IS NOT NULL THEN 1 ELSE 0 END), 0)::int,
			COALESCE(SUM(CASE WHEN maintenance_window_id IS NOT NULL AND NOT ok THEN 1 ELSE 0 END), 0)::int,
			COALESCE(SUM(CASE WHEN observer_suppressed = TRUE THEN 1 ELSE 0 END), 0)::int,
			MIN(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = FALSE THEN checked_at ELSE NULL END),
			MAX(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = FALSE THEN checked_at ELSE NULL END)
		FROM probe_results
		WHERE tenant_id = $1
			AND checked_at < $2
			GROUP BY monitor_id, 3
		ON CONFLICT(tenant_id, monitor_id, bucket_start) DO UPDATE SET
			total_checks = ` + target + `.total_checks + EXCLUDED.total_checks,
			successful_checks = ` + target + `.successful_checks + EXCLUDED.successful_checks,
			maintenance_checks = ` + target + `.maintenance_checks + EXCLUDED.maintenance_checks,
			maintenance_failed_checks = ` + target + `.maintenance_failed_checks + EXCLUDED.maintenance_failed_checks,
			observer_suppressed_checks = ` + target + `.observer_suppressed_checks + EXCLUDED.observer_suppressed_checks,
			first_reportable_at = COALESCE(LEAST(` + target + `.first_reportable_at, EXCLUDED.first_reportable_at), ` + target + `.first_reportable_at, EXCLUDED.first_reportable_at),
			last_reportable_at = COALESCE(GREATEST(` + target + `.last_reportable_at, EXCLUDED.last_reportable_at), ` + target + `.last_reportable_at, EXCLUDED.last_reportable_at)`
	_, err := tx.Exec(ctx, query, tenantID, cutoff)
	return err
}

func postgresRollupStoredProbeRollups(ctx context.Context, tx pgx.Tx, tenantID string, source string, target string, bucketExpr string, cutoff time.Time) error {
	query := `INSERT INTO ` + target + `
		(tenant_id, monitor_id, bucket_start, total_checks, successful_checks, maintenance_checks, maintenance_failed_checks,
			observer_suppressed_checks, first_reportable_at, last_reportable_at)
		SELECT tenant_id, monitor_id, ` + bucketExpr + ` AS bucket_start,
			COALESCE(SUM(total_checks), 0)::int,
			COALESCE(SUM(successful_checks), 0)::int,
			COALESCE(SUM(maintenance_checks), 0)::int,
			COALESCE(SUM(maintenance_failed_checks), 0)::int,
			COALESCE(SUM(observer_suppressed_checks), 0)::int,
			MIN(first_reportable_at),
			MAX(last_reportable_at)
		FROM ` + source + `
		WHERE tenant_id = $1
			AND bucket_start < $2
			GROUP BY tenant_id, monitor_id, 3
		ON CONFLICT(tenant_id, monitor_id, bucket_start) DO UPDATE SET
			total_checks = ` + target + `.total_checks + EXCLUDED.total_checks,
			successful_checks = ` + target + `.successful_checks + EXCLUDED.successful_checks,
			maintenance_checks = ` + target + `.maintenance_checks + EXCLUDED.maintenance_checks,
			maintenance_failed_checks = ` + target + `.maintenance_failed_checks + EXCLUDED.maintenance_failed_checks,
			observer_suppressed_checks = ` + target + `.observer_suppressed_checks + EXCLUDED.observer_suppressed_checks,
			first_reportable_at = COALESCE(LEAST(` + target + `.first_reportable_at, EXCLUDED.first_reportable_at), ` + target + `.first_reportable_at, EXCLUDED.first_reportable_at),
			last_reportable_at = COALESCE(GREATEST(` + target + `.last_reportable_at, EXCLUDED.last_reportable_at), ` + target + `.last_reportable_at, EXCLUDED.last_reportable_at)`
	_, err := tx.Exec(ctx, query, tenantID, cutoff)
	return err
}

func postgresMinuteBucketExpression(column string) string {
	return `date_trunc('minute', ` + column + `)`
}

func postgresHourlyBucketExpression(column string) string {
	return `date_trunc('hour', ` + column + `)`
}

func postgresDailyBucketExpression(column string) string {
	return `date_trunc('day', ` + column + `)`
}

func scanPostgresState(row pgx.Row) (MonitorState, error) {
	var state MonitorState
	var lastChecked, lastSuccess, lastFailure *time.Time
	err := row.Scan(
		&state.MonitorID, &state.Name, &state.URL, &state.ExpectedStatusCode, &state.Status, &state.StatusBeforeMaintenance, &state.ConsecutiveFailures,
		&lastChecked, &lastSuccess, &lastFailure, &state.LastError, &state.LastObservedStatusCode, &state.UpdatedAt,
	)
	if err != nil {
		return MonitorState{}, err
	}
	state.LastCheckedAt = derefTime(lastChecked)
	state.LastSuccessAt = derefTime(lastSuccess)
	state.LastFailureAt = derefTime(lastFailure)
	state.UpdatedAt = state.UpdatedAt.UTC()
	return state, nil
}

func scanPostgresObserverState(row pgx.Row) (ObserverState, error) {
	var state ObserverState
	var lastChecked, lastSuccess, lastFailure *time.Time
	err := row.Scan(
		&state.Status, &state.ConsecutiveFailures, &state.ConsecutiveSuccesses,
		&lastChecked, &lastSuccess, &lastFailure, &state.LastError, &state.UpdatedAt,
	)
	if err != nil {
		return ObserverState{}, err
	}
	state.LastCheckedAt = derefTime(lastChecked)
	state.LastSuccessAt = derefTime(lastSuccess)
	state.LastFailureAt = derefTime(lastFailure)
	state.UpdatedAt = state.UpdatedAt.UTC()
	return state, nil
}

func scanPostgresObserverSentinelResult(row pgx.Row) (ObserverSentinelResult, error) {
	var result ObserverSentinelResult
	err := row.Scan(
		&result.SentinelID, &result.Name, &result.URL, &result.ExpectedStatusCode,
		&result.OK, &result.ObservedStatusCode, &result.LatencyMS, &result.Error, &result.CheckedAt,
	)
	if err != nil {
		return ObserverSentinelResult{}, err
	}
	result.CheckedAt = result.CheckedAt.UTC()
	return result, nil
}

func scanPostgresFailedProbeResult(row pgx.Row) (ProbeResult, error) {
	var result ProbeResult
	var checkedAt *time.Time
	var maintenanceWindowID *int64
	err := row.Scan(
		&result.MonitorID, &checkedAt, &result.OK, &result.ObservedStatusCode,
		&result.LatencyMS, &result.ResponseTimeMS, &result.AttemptCount,
		&result.Error, &maintenanceWindowID, &result.ObserverSuppressed,
	)
	if err != nil {
		return ProbeResult{}, err
	}
	result.CheckedAt = derefTime(checkedAt)
	if maintenanceWindowID != nil {
		result.MaintenanceWindowID = *maintenanceWindowID
	}
	return result, nil
}

func scanPostgresAlertNotification(row pgx.Row) (AlertNotification, error) {
	var notification AlertNotification
	var nextRetry *time.Time
	if err := row.Scan(&notification.ID, &notification.IncidentID, &notification.MonitorID, &notification.Provider, &notification.AttemptedAt, &notification.AttemptNumber, &notification.Success, &notification.Error, &nextRetry, &notification.RetryExhausted); err != nil {
		return AlertNotification{}, err
	}
	notification.AttemptedAt = notification.AttemptedAt.UTC()
	notification.NextRetryAt = derefTime(nextRetry)
	return notification, nil
}

func scanPostgresMaintenanceWindow(row pgx.Row) (MaintenanceWindow, error) {
	var window MaintenanceWindow
	var cancelledAt *time.Time
	err := row.Scan(
		&window.ID, &window.MonitorID, &window.StartsAt, &window.EndsAt, &window.Reason, &window.CreatedBy, &window.CreatedAt,
		&cancelledAt, &window.CancelledBy, &window.CancellationReason,
	)
	if err != nil {
		return MaintenanceWindow{}, err
	}
	window.StartsAt = window.StartsAt.UTC()
	window.EndsAt = window.EndsAt.UTC()
	window.CreatedAt = window.CreatedAt.UTC()
	window.CancelledAt = derefTime(cancelledAt)
	return window, nil
}

func postgresNullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

func derefTime(t *time.Time) time.Time {
	if t == nil || t.IsZero() {
		return time.Time{}
	}
	return t.UTC()
}
