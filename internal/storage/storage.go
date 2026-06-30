package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"upag/internal/config"
	"upag/internal/state"

	_ "github.com/mattn/go-sqlite3"
)

type Backend interface {
	Close() error
	GetState(context.Context, string) (MonitorState, bool, error)
	SaveProbeAndState(context.Context, ProbeResult, MonitorState, *Incident) (int64, error)
	SaveProbeResult(context.Context, ProbeResult) error
	GetObserverState(context.Context) (ObserverState, bool, error)
	ListObserverSentinelResults(context.Context) ([]ObserverSentinelResult, error)
	SaveObserverCheck(context.Context, ObserverState, []ObserverSentinelResult, *Incident) (int64, error)
	SaveAlertNotifications(context.Context, []AlertNotification) error
	ListStates(context.Context) ([]MonitorState, error)
	EnsureStatusIntervalsBackfilled(context.Context, FailureThresholds) error
	ListStatusIntervals(context.Context, StatusIntervalFilter) ([]StatusInterval, error)
	ListUptimeStats(context.Context, time.Time, FailureThresholds) (map[string]UptimeStats, error)
	ListIncidents(context.Context, int) ([]Incident, error)
	ListFailedProbeResults(context.Context, int) ([]ProbeResult, error)
	ListObserverSentinelEvents(context.Context, int) ([]ObserverSentinelResult, error)
	ListAlertNotifications(context.Context, int) ([]AlertNotification, error)
	ListActionableAlertDeliveryFailures(context.Context, int) ([]AlertNotification, error)
	ListDueAlertNotificationRetries(context.Context, time.Time, int) ([]AlertNotificationRetry, error)
	AddMaintenanceWindow(context.Context, MaintenanceWindow) (int64, error)
	CancelMaintenanceWindow(context.Context, int64, time.Time, string, string) error
	ActiveMaintenanceWindow(context.Context, string, time.Time) (MaintenanceWindow, bool, error)
	ListMaintenanceWindows(context.Context, MaintenanceWindowFilter) ([]MaintenanceWindow, error)
	DeleteStatesExcept(context.Context, []string) error
	PruneProbeResults(context.Context, time.Duration, time.Time) error
	RollupAndPruneProbeResults(context.Context, ProbeRetentionPolicy, time.Time) error
}

const defaultTenantID = "default"

type tenantContextKey struct{}

func WithTenant(ctx context.Context, tenantID string) context.Context {
	if strings.TrimSpace(tenantID) == "" {
		tenantID = defaultTenantID
	}
	return context.WithValue(ctx, tenantContextKey{}, tenantID)
}

func TenantFromContext(ctx context.Context) string {
	if ctx == nil {
		return defaultTenantID
	}
	tenant, ok := ctx.Value(tenantContextKey{}).(string)
	if !ok || strings.TrimSpace(tenant) == "" {
		return defaultTenantID
	}
	return tenant
}

type Store struct {
	db *sql.DB
}

type migration struct {
	ID string
	Fn func(context.Context, *sql.Tx) error
}

type MonitorState struct {
	MonitorID               string
	Name                    string
	URL                     string
	ExpectedStatusCode      int
	Status                  string
	StatusBeforeMaintenance string
	ConsecutiveFailures     int
	LastCheckedAt           time.Time
	LastSuccessAt           time.Time
	LastFailureAt           time.Time
	LastError               string
	LastObservedStatusCode  int
	UpdatedAt               time.Time
}

type ProbeResult struct {
	MonitorID           string
	CheckedAt           time.Time
	OK                  bool
	ObservedStatusCode  int
	LatencyMS           int64
	ResponseTimeMS      int64
	AttemptCount        int
	Error               string
	MaintenanceWindowID int64
	ObserverSuppressed  bool
}

type ObserverState struct {
	Status               string
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	LastCheckedAt        time.Time
	LastSuccessAt        time.Time
	LastFailureAt        time.Time
	LastError            string
	UpdatedAt            time.Time
}

type ObserverSentinelResult struct {
	SentinelID         string
	Name               string
	URL                string
	ExpectedStatusCode int
	OK                 bool
	ObservedStatusCode int
	LatencyMS          int64
	Error              string
	CheckedAt          time.Time
}

type UptimeStats struct {
	TwentyFourHour UptimeWindowStats
	SevenDay       UptimeWindowStats
	ThirtyDay      UptimeWindowStats
	Retained       UptimeWindowStats
}

type UptimeWindowStats struct {
	TotalChecks             int
	SuccessfulChecks        int
	FailedChecks            int
	MaintenanceChecks       int
	MaintenanceFailedChecks int
	DowntimeSeconds         int64
	ReportableSeconds       int64
	WindowStartedAt         time.Time
	WindowEndedAt           time.Time
}

type StatusInterval struct {
	ID        int64
	MonitorID string
	Status    string
	StartedAt time.Time
	EndedAt   time.Time
	Downtime  bool
}

type StatusIntervalFilter struct {
	MonitorID string
	Limit     int
}

type FailureThresholds struct {
	Default  int
	Monitors map[string]int
}

func SingleFailureThreshold(threshold int) FailureThresholds {
	return FailureThresholds{Default: threshold}
}

func (t FailureThresholds) ForMonitor(monitorID string) int {
	if threshold := t.Monitors[monitorID]; threshold > 0 {
		return threshold
	}
	if t.Default > 0 {
		return t.Default
	}
	return 1
}

type RollupRetention struct {
	Duration time.Duration
	Forever  bool
}

type ProbeRetentionPolicy struct {
	ProbeResults       RollupRetention
	ProbeMinuteRollups RollupRetention
	ProbeHourlyRollups RollupRetention
	ProbeDailyRollups  RollupRetention
}

type Incident struct {
	ID         int64
	MonitorID  string
	Name       string
	Transition string
	ObservedAt time.Time
	Error      string
	StatusCode int
}

type AlertNotification struct {
	ID             int64
	IncidentID     int64
	MonitorID      string
	Provider       string
	AttemptedAt    time.Time
	AttemptNumber  int
	Success        bool
	Error          string
	NextRetryAt    time.Time
	RetryExhausted bool
}

type AlertNotificationRetry struct {
	Notification AlertNotification
	Incident     Incident
	CurrentState MonitorState
}

type MaintenanceWindow struct {
	ID                 int64
	MonitorID          string
	StartsAt           time.Time
	EndsAt             time.Time
	Reason             string
	CreatedBy          string
	CreatedAt          time.Time
	CancelledAt        time.Time
	CancelledBy        string
	CancellationReason string
}

type MaintenanceWindowFilter struct {
	MonitorID  string
	IncludeAll bool
	Now        time.Time
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", path+"?_foreign_keys=on&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	store := &Store{db: db}
	if err := store.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func OpenBackend(ctx context.Context, cfg config.StorageConfig) (Backend, error) {
	switch cfg.Backend {
	case "sqlite":
		return Open(cfg.SQLite.Path)
	case "postgres":
		return OpenPostgres(ctx, cfg.Postgres.DSN)
	default:
		return nil, fmt.Errorf("unsupported storage backend %q", cfg.Backend)
	}
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		id TEXT PRIMARY KEY,
		applied_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := s.appliedMigrations(ctx)
	if err != nil {
		return err
	}
	for _, migration := range migrations() {
		if applied[migration.ID] {
			continue
		}
		if err := s.applyMigration(ctx, migration); err != nil {
			return err
		}
	}
	return nil
}

func migrations() []migration {
	return []migration{
		{ID: "0001_initial_schema", Fn: migrateInitialSchema},
		{ID: "0002_alert_notification_retries", Fn: migrateAlertNotificationRetries},
		{ID: "0003_maintenance_windows", Fn: migrateMaintenanceWindows},
		{ID: "0004_monitor_status_before_maintenance", Fn: migrateMonitorStatusBeforeMaintenance},
		{ID: "0005_probe_response_time_and_maintenance", Fn: migrateProbeResponseTimeAndMaintenance},
		{ID: "0006_probe_attempt_count", Fn: migrateProbeAttemptCount},
		{ID: "0007_observer_health", Fn: migrateObserverHealth},
		{ID: "0008_probe_rollups", Fn: migrateProbeRollups},
		{ID: "0009_observer_sentinel_events", Fn: migrateObserverSentinelEvents},
		{ID: "0010_monitor_status_intervals", Fn: migrateMonitorStatusIntervals},
	}
}

func (s *Store) appliedMigrations(ctx context.Context) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("list schema migrations: %w", err)
	}
	defer rows.Close()

	applied := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		applied[id] = true
	}
	return applied, rows.Err()
}

func (s *Store) applyMigration(ctx context.Context, migration migration) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", migration.ID, err)
	}
	defer tx.Rollback()

	if err := migration.Fn(ctx, tx); err != nil {
		return fmt.Errorf("apply migration %s: %w", migration.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (id, applied_at) VALUES (?, ?)`, migration.ID, formatTime(time.Now().UTC())); err != nil {
		return fmt.Errorf("record migration %s: %w", migration.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", migration.ID, err)
	}
	return nil
}

func migrateInitialSchema(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS monitor_states (
			monitor_id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			url TEXT NOT NULL,
			expected_status_code INTEGER NOT NULL,
			status TEXT NOT NULL,
			consecutive_failures INTEGER NOT NULL,
			last_checked_at TEXT,
			last_success_at TEXT,
			last_failure_at TEXT,
			last_error TEXT NOT NULL,
			last_observed_status_code INTEGER NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS probe_results (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			monitor_id TEXT NOT NULL,
			checked_at TEXT NOT NULL,
			ok INTEGER NOT NULL,
			observed_status_code INTEGER NOT NULL,
			latency_ms INTEGER NOT NULL,
			error TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_probe_results_monitor_checked
			ON probe_results (monitor_id, checked_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_probe_results_checked
			ON probe_results (checked_at)`,
		`CREATE TABLE IF NOT EXISTS incidents (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			monitor_id TEXT NOT NULL,
			name TEXT NOT NULL,
			transition TEXT NOT NULL,
			observed_at TEXT NOT NULL,
			error TEXT NOT NULL,
			status_code INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_incidents_observed
			ON incidents (observed_at DESC)`,
		`CREATE TABLE IF NOT EXISTS alert_notifications (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			incident_id INTEGER NOT NULL,
			monitor_id TEXT NOT NULL,
			provider TEXT NOT NULL,
			attempted_at TEXT NOT NULL,
			success INTEGER NOT NULL,
			error TEXT NOT NULL,
			FOREIGN KEY (incident_id) REFERENCES incidents(id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_alert_notifications_incident
			ON alert_notifications (incident_id)`,
	}
	return execMigrationStatements(ctx, tx, statements)
}

func migrateAlertNotificationRetries(ctx context.Context, tx *sql.Tx) error {
	if err := addColumnIfMissing(ctx, tx, "alert_notifications", "attempt_number", `ALTER TABLE alert_notifications ADD COLUMN attempt_number INTEGER NOT NULL DEFAULT 1`); err != nil {
		return err
	}
	if err := addColumnIfMissing(ctx, tx, "alert_notifications", "next_retry_at", `ALTER TABLE alert_notifications ADD COLUMN next_retry_at TEXT`); err != nil {
		return err
	}
	if err := addColumnIfMissing(ctx, tx, "alert_notifications", "retry_exhausted", `ALTER TABLE alert_notifications ADD COLUMN retry_exhausted INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	return execMigrationStatements(ctx, tx, []string{
		`CREATE INDEX IF NOT EXISTS idx_alert_notifications_monitor_attempted
			ON alert_notifications (monitor_id, attempted_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_alert_notifications_attempted
			ON alert_notifications (attempted_at DESC)`,
	})
}

func migrateMaintenanceWindows(ctx context.Context, tx *sql.Tx) error {
	return execMigrationStatements(ctx, tx, []string{
		`CREATE TABLE IF NOT EXISTS maintenance_windows (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			monitor_id TEXT NOT NULL,
			starts_at TEXT NOT NULL,
			ends_at TEXT NOT NULL,
			reason TEXT NOT NULL,
			created_by TEXT NOT NULL,
			created_at TEXT NOT NULL,
			cancelled_at TEXT,
			cancelled_by TEXT NOT NULL DEFAULT '',
			cancellation_reason TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_maintenance_windows_monitor_time
			ON maintenance_windows (monitor_id, starts_at, ends_at)`,
	})
}

func migrateMonitorStatusBeforeMaintenance(ctx context.Context, tx *sql.Tx) error {
	return addColumnIfMissing(ctx, tx, "monitor_states", "status_before_maintenance", `ALTER TABLE monitor_states ADD COLUMN status_before_maintenance TEXT NOT NULL DEFAULT ''`)
}

func migrateProbeResponseTimeAndMaintenance(ctx context.Context, tx *sql.Tx) error {
	if err := addColumnIfMissing(ctx, tx, "probe_results", "response_time_ms", `ALTER TABLE probe_results ADD COLUMN response_time_ms INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	return addColumnIfMissing(ctx, tx, "probe_results", "maintenance_window_id", `ALTER TABLE probe_results ADD COLUMN maintenance_window_id INTEGER`)
}

func migrateProbeAttemptCount(ctx context.Context, tx *sql.Tx) error {
	return addColumnIfMissing(ctx, tx, "probe_results", "attempt_count", `ALTER TABLE probe_results ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 1`)
}

func migrateObserverHealth(ctx context.Context, tx *sql.Tx) error {
	if err := addColumnIfMissing(ctx, tx, "probe_results", "observer_suppressed", `ALTER TABLE probe_results ADD COLUMN observer_suppressed INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	return execMigrationStatements(ctx, tx, []string{
		`CREATE TABLE IF NOT EXISTS observer_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			status TEXT NOT NULL,
			consecutive_failures INTEGER NOT NULL,
			consecutive_successes INTEGER NOT NULL,
			last_checked_at TEXT,
			last_success_at TEXT,
			last_failure_at TEXT,
			last_error TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS observer_sentinel_results (
			sentinel_id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			url TEXT NOT NULL,
			expected_status_code INTEGER NOT NULL,
			ok INTEGER NOT NULL,
			observed_status_code INTEGER NOT NULL,
			latency_ms INTEGER NOT NULL,
			error TEXT NOT NULL,
			checked_at TEXT NOT NULL
		)`,
	})
}

func migrateProbeRollups(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		rollupTableStatement("probe_minute_rollups"),
		rollupTableStatement("probe_hourly_rollups"),
		rollupTableStatement("probe_daily_rollups"),
		`CREATE TABLE IF NOT EXISTS probe_outcome_runs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			monitor_id TEXT NOT NULL,
			started_at TEXT NOT NULL,
			ended_at TEXT NOT NULL,
			ok INTEGER NOT NULL,
			probe_count INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_probe_outcome_runs_monitor_started
			ON probe_outcome_runs (monitor_id, started_at)`,
	}
	return execMigrationStatements(ctx, tx, statements)
}

func rollupTableStatement(name string) string {
	return `CREATE TABLE IF NOT EXISTS ` + name + ` (
		monitor_id TEXT NOT NULL,
		bucket_start TEXT NOT NULL,
		total_checks INTEGER NOT NULL,
		successful_checks INTEGER NOT NULL,
		maintenance_checks INTEGER NOT NULL,
		maintenance_failed_checks INTEGER NOT NULL,
		observer_suppressed_checks INTEGER NOT NULL,
		first_reportable_at TEXT,
		last_reportable_at TEXT,
		PRIMARY KEY (monitor_id, bucket_start)
	)`
}

func migrateObserverSentinelEvents(ctx context.Context, tx *sql.Tx) error {
	return execMigrationStatements(ctx, tx, []string{
		`CREATE TABLE IF NOT EXISTS observer_sentinel_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			sentinel_id TEXT NOT NULL,
			name TEXT NOT NULL,
			url TEXT NOT NULL,
			expected_status_code INTEGER NOT NULL,
			ok INTEGER NOT NULL,
			observed_status_code INTEGER NOT NULL,
			latency_ms INTEGER NOT NULL,
			error TEXT NOT NULL,
			checked_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_observer_sentinel_events_checked
			ON observer_sentinel_events (checked_at DESC)`,
	})
}

func migrateMonitorStatusIntervals(ctx context.Context, tx *sql.Tx) error {
	return execMigrationStatements(ctx, tx, []string{
		`CREATE TABLE IF NOT EXISTS monitor_status_intervals (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			monitor_id TEXT NOT NULL,
			status TEXT NOT NULL,
			started_at TEXT NOT NULL,
			ended_at TEXT,
			downtime INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_monitor_status_intervals_monitor_started
			ON monitor_status_intervals (monitor_id, started_at)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_monitor_status_intervals_open
			ON monitor_status_intervals (monitor_id)
			WHERE ended_at IS NULL`,
		`CREATE TABLE IF NOT EXISTS monitor_status_interval_backfills (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			backfilled_at TEXT NOT NULL,
			failure_threshold INTEGER NOT NULL
		)`,
	})
}

func execMigrationStatements(ctx context.Context, tx *sql.Tx, statements []string) error {
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate sqlite database: %w", err)
		}
	}
	return nil
}

func addColumnIfMissing(ctx context.Context, tx *sql.Tx, table string, column string, statement string) error {
	columns, err := tableColumns(ctx, tx, table)
	if err != nil {
		return err
	}
	if columns[column] {
		return nil
	}
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("migrate %s.%s: %w", table, column, err)
	}
	return nil
}

func (s *Store) tableColumns(ctx context.Context, table string) (map[string]bool, error) {
	return tableColumns(ctx, s.db, table)
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func tableColumns(ctx context.Context, db queryer, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := map[string]bool{}
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return nil, err
		}
		columns[name] = true
	}
	return columns, rows.Err()
}

func (s *Store) GetState(ctx context.Context, monitorID string) (MonitorState, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT
		monitor_id, name, url, expected_status_code, status, status_before_maintenance, consecutive_failures,
		last_checked_at, last_success_at, last_failure_at, last_error,
		last_observed_status_code, updated_at
		FROM monitor_states WHERE monitor_id = ?`, monitorID)
	state, err := scanState(row)
	if err == sql.ErrNoRows {
		return MonitorState{}, false, nil
	}
	if err != nil {
		return MonitorState{}, false, err
	}
	return state, true, nil
}

func (s *Store) SaveProbeAndState(ctx context.Context, result ProbeResult, next MonitorState, incident *Incident) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	attemptCount := result.AttemptCount
	if attemptCount == 0 {
		attemptCount = 1
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO probe_results
		(monitor_id, checked_at, ok, observed_status_code, latency_ms, response_time_ms, attempt_count, error, maintenance_window_id, observer_suppressed)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		result.MonitorID, formatTime(result.CheckedAt), boolInt(result.OK), result.ObservedStatusCode, result.LatencyMS, result.ResponseTimeMS, attemptCount, result.Error, nullableInt64(result.MaintenanceWindowID), boolInt(result.ObserverSuppressed)); err != nil {
		return 0, fmt.Errorf("insert probe result: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO monitor_states
		(monitor_id, name, url, expected_status_code, status, status_before_maintenance, consecutive_failures,
		last_checked_at, last_success_at, last_failure_at, last_error,
		last_observed_status_code, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(monitor_id) DO UPDATE SET
			name = excluded.name,
			url = excluded.url,
			expected_status_code = excluded.expected_status_code,
			status = excluded.status,
			status_before_maintenance = excluded.status_before_maintenance,
			consecutive_failures = excluded.consecutive_failures,
			last_checked_at = excluded.last_checked_at,
			last_success_at = excluded.last_success_at,
			last_failure_at = excluded.last_failure_at,
			last_error = excluded.last_error,
			last_observed_status_code = excluded.last_observed_status_code,
			updated_at = excluded.updated_at`,
		next.MonitorID, next.Name, next.URL, next.ExpectedStatusCode, next.Status, next.StatusBeforeMaintenance, next.ConsecutiveFailures,
		formatTime(next.LastCheckedAt), formatTime(next.LastSuccessAt), formatTime(next.LastFailureAt), next.LastError,
		next.LastObservedStatusCode, formatTime(next.UpdatedAt)); err != nil {
		return 0, fmt.Errorf("save monitor state: %w", err)
	}
	if err := saveStatusInterval(ctx, tx, next.MonitorID, next.Status, next.UpdatedAt); err != nil {
		return 0, fmt.Errorf("save monitor status interval: %w", err)
	}

	var incidentID int64
	if incident != nil {
		result, err := tx.ExecContext(ctx, `INSERT INTO incidents
			(monitor_id, name, transition, observed_at, error, status_code)
			VALUES (?, ?, ?, ?, ?, ?)`,
			incident.MonitorID, incident.Name, incident.Transition, formatTime(incident.ObservedAt), incident.Error, incident.StatusCode)
		if err != nil {
			return 0, fmt.Errorf("insert incident: %w", err)
		}
		incidentID, err = result.LastInsertId()
		if err != nil {
			return 0, fmt.Errorf("get incident id: %w", err)
		}
		incident.ID = incidentID
	}

	return incidentID, tx.Commit()
}

func saveStatusInterval(ctx context.Context, tx *sql.Tx, monitorID string, status string, observedAt time.Time) error {
	observedAt = observedAt.UTC()
	var id int64
	var previousStatus string
	err := tx.QueryRowContext(ctx, `SELECT id, status
		FROM monitor_status_intervals
		WHERE monitor_id = ? AND ended_at IS NULL
		ORDER BY started_at DESC, id DESC LIMIT 1`, monitorID).Scan(&id, &previousStatus)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == nil && previousStatus == status {
		return nil
	}
	downtime := statusCountsAsDowntime(status)
	if err == sql.ErrNoRows {
		_, err = tx.ExecContext(ctx, `INSERT INTO monitor_status_intervals
			(monitor_id, status, started_at, ended_at, downtime)
			VALUES (?, ?, ?, NULL, ?)`, monitorID, status, formatTime(observedAt), boolInt(downtime))
		return err
	}
	previousDowntime := statusCountsAsDowntime(previousStatus) || (previousStatus == state.Failing && status == state.Down)
	if _, err := tx.ExecContext(ctx, `UPDATE monitor_status_intervals
		SET ended_at = ?, downtime = ?
		WHERE id = ?`, formatTime(observedAt), boolInt(previousDowntime), id); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO monitor_status_intervals
		(monitor_id, status, started_at, ended_at, downtime)
		VALUES (?, ?, ?, NULL, ?)`, monitorID, status, formatTime(observedAt), boolInt(downtime))
	return err
}

func statusCountsAsDowntime(status string) bool {
	return status == state.Down
}

func (s *Store) SaveProbeResult(ctx context.Context, result ProbeResult) error {
	attemptCount := result.AttemptCount
	if attemptCount == 0 {
		attemptCount = 1
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO probe_results
		(monitor_id, checked_at, ok, observed_status_code, latency_ms, response_time_ms, attempt_count, error, maintenance_window_id, observer_suppressed)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		result.MonitorID, formatTime(result.CheckedAt), boolInt(result.OK), result.ObservedStatusCode, result.LatencyMS, result.ResponseTimeMS, attemptCount, result.Error, nullableInt64(result.MaintenanceWindowID), boolInt(result.ObserverSuppressed))
	if err != nil {
		return fmt.Errorf("insert probe result: %w", err)
	}
	return nil
}

func (s *Store) GetObserverState(ctx context.Context) (ObserverState, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT
		status, consecutive_failures, consecutive_successes, last_checked_at,
		last_success_at, last_failure_at, last_error, updated_at
		FROM observer_state WHERE id = 1`)
	state, err := scanObserverState(row)
	if err == sql.ErrNoRows {
		return ObserverState{}, false, nil
	}
	if err != nil {
		return ObserverState{}, false, err
	}
	return state, true, nil
}

func (s *Store) ListObserverSentinelResults(ctx context.Context) ([]ObserverSentinelResult, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT
		sentinel_id, name, url, expected_status_code, ok, observed_status_code,
		latency_ms, error, checked_at
		FROM observer_sentinel_results ORDER BY sentinel_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []ObserverSentinelResult
	for rows.Next() {
		result, err := scanObserverSentinelResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func (s *Store) SaveObserverCheck(ctx context.Context, state ObserverState, results []ObserverSentinelResult, incident *Incident) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `INSERT INTO observer_state
		(id, status, consecutive_failures, consecutive_successes, last_checked_at,
		 last_success_at, last_failure_at, last_error, updated_at)
		VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			status = excluded.status,
			consecutive_failures = excluded.consecutive_failures,
			consecutive_successes = excluded.consecutive_successes,
			last_checked_at = excluded.last_checked_at,
			last_success_at = excluded.last_success_at,
			last_failure_at = excluded.last_failure_at,
			last_error = excluded.last_error,
			updated_at = excluded.updated_at`,
		state.Status, state.ConsecutiveFailures, state.ConsecutiveSuccesses, formatTime(state.LastCheckedAt),
		formatTime(state.LastSuccessAt), formatTime(state.LastFailureAt), state.LastError, formatTime(state.UpdatedAt)); err != nil {
		return 0, fmt.Errorf("save observer state: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM observer_sentinel_results`); err != nil {
		return 0, fmt.Errorf("replace observer sentinel results: %w", err)
	}
	for _, result := range results {
		if _, err := tx.ExecContext(ctx, `INSERT INTO observer_sentinel_results
			(sentinel_id, name, url, expected_status_code, ok, observed_status_code, latency_ms, error, checked_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			result.SentinelID, result.Name, result.URL, result.ExpectedStatusCode, boolInt(result.OK),
			result.ObservedStatusCode, result.LatencyMS, result.Error, formatTime(result.CheckedAt)); err != nil {
			return 0, fmt.Errorf("save observer sentinel result: %w", err)
		}
		if !result.OK {
			if _, err := tx.ExecContext(ctx, `INSERT INTO observer_sentinel_events
				(sentinel_id, name, url, expected_status_code, ok, observed_status_code, latency_ms, error, checked_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				result.SentinelID, result.Name, result.URL, result.ExpectedStatusCode, boolInt(result.OK),
				result.ObservedStatusCode, result.LatencyMS, result.Error, formatTime(result.CheckedAt)); err != nil {
				return 0, fmt.Errorf("save observer sentinel event: %w", err)
			}
		}
	}

	var incidentID int64
	if incident != nil {
		result, err := tx.ExecContext(ctx, `INSERT INTO incidents
			(monitor_id, name, transition, observed_at, error, status_code)
			VALUES (?, ?, ?, ?, ?, ?)`,
			incident.MonitorID, incident.Name, incident.Transition, formatTime(incident.ObservedAt), incident.Error, incident.StatusCode)
		if err != nil {
			return 0, fmt.Errorf("insert incident: %w", err)
		}
		incidentID, err = result.LastInsertId()
		if err != nil {
			return 0, fmt.Errorf("get incident id: %w", err)
		}
		incident.ID = incidentID
	}

	return incidentID, tx.Commit()
}

func (s *Store) SaveAlertNotifications(ctx context.Context, notifications []AlertNotification) error {
	if len(notifications) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, notification := range notifications {
		attemptNumber := notification.AttemptNumber
		if attemptNumber == 0 {
			attemptNumber = 1
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO alert_notifications
			(incident_id, monitor_id, provider, attempted_at, attempt_number, success, error, next_retry_at, retry_exhausted)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			notification.IncidentID, notification.MonitorID, notification.Provider, formatTime(notification.AttemptedAt),
			attemptNumber, boolInt(notification.Success), notification.Error, formatTime(notification.NextRetryAt),
			boolInt(notification.RetryExhausted)); err != nil {
			return fmt.Errorf("insert alert notification: %w", err)
		}
	}
	return tx.Commit()
}

func (s *Store) ListStates(ctx context.Context) ([]MonitorState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT
		monitor_id, name, url, expected_status_code, status, status_before_maintenance, consecutive_failures,
		last_checked_at, last_success_at, last_failure_at, last_error,
		last_observed_status_code, updated_at
		FROM monitor_states ORDER BY last_checked_at DESC, monitor_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var states []MonitorState
	for rows.Next() {
		state, err := scanState(rows)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, rows.Err()
}

func (s *Store) ListStatusIntervals(ctx context.Context, filter StatusIntervalFilter) ([]StatusInterval, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	query := `SELECT id, monitor_id, status, started_at, ended_at, downtime
		FROM monitor_status_intervals`
	args := []any{}
	if strings.TrimSpace(filter.MonitorID) != "" {
		query += ` WHERE monitor_id = ?`
		args = append(args, strings.TrimSpace(filter.MonitorID))
	}
	query += ` ORDER BY started_at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var intervals []StatusInterval
	for rows.Next() {
		interval, err := scanStatusInterval(rows)
		if err != nil {
			return nil, err
		}
		intervals = append(intervals, interval)
	}
	return intervals, rows.Err()
}

func (s *Store) ListUptimeStats(ctx context.Context, now time.Time, thresholds FailureThresholds) (map[string]UptimeStats, error) {
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

func (s *Store) listUptimeWindowStats(ctx context.Context, cutoff time.Time) (map[string]UptimeWindowStats, error) {
	query, args := uptimeWindowQuery(cutoff)
	rows, err := s.db.QueryContext(ctx, query, args...)
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
		var startedAt sql.NullString
		var endedAt sql.NullString
		if err := rows.Scan(&monitorID, &totalChecks, &successfulChecks, &maintenanceChecks, &maintenanceFailedChecks, &startedAt, &endedAt); err != nil {
			return nil, err
		}
		stats[monitorID] = UptimeWindowStats{
			TotalChecks:             totalChecks,
			SuccessfulChecks:        successfulChecks,
			FailedChecks:            totalChecks - successfulChecks,
			MaintenanceChecks:       maintenanceChecks,
			MaintenanceFailedChecks: maintenanceFailedChecks,
			WindowStartedAt:         parseNullTime(startedAt),
			WindowEndedAt:           parseNullTime(endedAt),
		}
	}
	return stats, rows.Err()
}

func uptimeWindowQuery(cutoff time.Time) (string, []any) {
	rawWhere := ""
	rollupWhere := ""
	args := []any{}
	if !cutoff.IsZero() {
		rawWhere = ` WHERE checked_at >= ?`
		rollupWhere = ` WHERE bucket_start >= ?`
		formatted := formatTime(cutoff)
		args = append(args, formatted, formatted, formatted, formatted)
	}
	query := `SELECT monitor_id,
			COALESCE(SUM(total_checks), 0),
			COALESCE(SUM(successful_checks), 0),
			COALESCE(SUM(maintenance_checks), 0),
			COALESCE(SUM(maintenance_failed_checks), 0),
			MIN(first_reportable_at),
			MAX(last_reportable_at)
		FROM (
			SELECT monitor_id,
				COALESCE(SUM(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = 0 THEN 1 ELSE 0 END), 0) AS total_checks,
				COALESCE(SUM(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = 0 THEN ok ELSE 0 END), 0) AS successful_checks,
				COALESCE(SUM(CASE WHEN maintenance_window_id IS NOT NULL THEN 1 ELSE 0 END), 0) AS maintenance_checks,
				COALESCE(SUM(CASE WHEN maintenance_window_id IS NOT NULL AND ok = 0 THEN 1 ELSE 0 END), 0) AS maintenance_failed_checks,
				MIN(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = 0 THEN checked_at ELSE NULL END) AS first_reportable_at,
				MAX(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = 0 THEN checked_at ELSE NULL END) AS last_reportable_at
			FROM probe_results` + rawWhere + `
			GROUP BY monitor_id
			UNION ALL
			SELECT monitor_id, total_checks, successful_checks, maintenance_checks, maintenance_failed_checks, first_reportable_at, last_reportable_at
			FROM probe_minute_rollups` + rollupWhere + `
			UNION ALL
			SELECT monitor_id, total_checks, successful_checks, maintenance_checks, maintenance_failed_checks, first_reportable_at, last_reportable_at
			FROM probe_hourly_rollups` + rollupWhere + `
			UNION ALL
			SELECT monitor_id, total_checks, successful_checks, maintenance_checks, maintenance_failed_checks, first_reportable_at, last_reportable_at
			FROM probe_daily_rollups` + rollupWhere + `
		)
		GROUP BY monitor_id`
	return query, args
}

type uptimeProbe struct {
	MonitorID string
	CheckedAt time.Time
	OK        bool
}

type uptimeRun struct {
	MonitorID  string
	StartedAt  time.Time
	EndedAt    time.Time
	OK         bool
	ProbeCount int
}

type timeInterval struct {
	Start time.Time
	End   time.Time
}

func (s *Store) addStrictAvailability(ctx context.Context, now time.Time, stats map[string]UptimeStats) error {
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
		maintenanceByMonitor[window.MonitorID] = append(maintenanceByMonitor[window.MonitorID], timeInterval{
			Start: window.StartsAt,
			End:   window.EndsAt,
		})
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

func (s *Store) listDowntimeIntervals(ctx context.Context, now time.Time) (map[string][]timeInterval, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT monitor_id, started_at, ended_at
		FROM monitor_status_intervals
		WHERE downtime = 1
		ORDER BY monitor_id, started_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	outages := map[string][]timeInterval{}
	for rows.Next() {
		var monitorID string
		var startedAt string
		var endedAt sql.NullString
		if err := rows.Scan(&monitorID, &startedAt, &endedAt); err != nil {
			return nil, err
		}
		end := parseNullTime(endedAt)
		if end.IsZero() {
			end = now
		}
		outages[monitorID] = append(outages[monitorID], timeInterval{
			Start: parseTime(startedAt),
			End:   end,
		})
	}
	return outages, rows.Err()
}

func (s *Store) EnsureStatusIntervalsBackfilled(ctx context.Context, thresholds FailureThresholds) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var marker int
	err = tx.QueryRowContext(ctx, `SELECT id FROM monitor_status_interval_backfills WHERE id = 1`).Scan(&marker)
	if err == nil {
		return tx.Commit()
	}
	if err != sql.ErrNoRows {
		return err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM monitor_status_intervals`); err != nil {
		return err
	}
	runs, err := listReportableUptimeRunsTx(ctx, tx)
	if err != nil {
		return err
	}
	if err := backfillStatusIntervalsFromRuns(ctx, tx, runs, thresholds); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO monitor_status_interval_backfills
		(id, backfilled_at, failure_threshold)
		VALUES (1, ?, ?)`, formatTime(time.Now().UTC()), thresholds.ForMonitor("")); err != nil {
		return err
	}
	return tx.Commit()
}

func listReportableUptimeRunsTx(ctx context.Context, tx *sql.Tx) ([]uptimeRun, error) {
	rows, err := tx.QueryContext(ctx, `SELECT monitor_id, started_at, ended_at, ok, probe_count
		FROM probe_outcome_runs
		UNION ALL
		SELECT monitor_id, checked_at AS started_at, checked_at AS ended_at, ok, 1 AS probe_count
		FROM probe_results
		WHERE maintenance_window_id IS NULL
			AND observer_suppressed = 0
		ORDER BY monitor_id ASC, started_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []uptimeRun
	for rows.Next() {
		var run uptimeRun
		var startedAt string
		var endedAt string
		var ok int
		if err := rows.Scan(&run.MonitorID, &startedAt, &endedAt, &ok, &run.ProbeCount); err != nil {
			return nil, err
		}
		run.StartedAt = parseTime(startedAt)
		run.EndedAt = parseTime(endedAt)
		run.OK = intBool(ok)
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

func backfillStatusIntervalsFromRuns(ctx context.Context, tx *sql.Tx, runs []uptimeRun, thresholds FailureThresholds) error {
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
			endedAt = formatTime(runs[i+1].StartedAt)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO monitor_status_intervals
			(monitor_id, status, started_at, ended_at, downtime)
			VALUES (?, ?, ?, ?, ?)`,
			run.MonitorID, statusValue, formatTime(run.StartedAt), endedAt, boolInt(statusCountsAsDowntime(statusValue))); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) listUncancelledMaintenanceWindows(ctx context.Context) ([]MaintenanceWindow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT
		id, monitor_id, starts_at, ends_at, reason, created_by, created_at,
		cancelled_at, cancelled_by, cancellation_reason
		FROM maintenance_windows
		WHERE cancelled_at IS NULL
		ORDER BY monitor_id ASC, starts_at ASC, id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var windows []MaintenanceWindow
	for rows.Next() {
		window, err := scanMaintenanceWindow(rows)
		if err != nil {
			return nil, err
		}
		windows = append(windows, window)
	}
	return windows, rows.Err()
}

func applyAvailabilityWindow(stats UptimeWindowStats, firstProbe time.Time, cutoff time.Time, now time.Time, outages []timeInterval, maintenance []timeInterval) UptimeWindowStats {
	start := firstProbe
	if !cutoff.IsZero() && cutoff.After(start) {
		start = cutoff
	}
	if !now.After(start) {
		stats.ReportableSeconds = 0
		stats.DowntimeSeconds = 0
		return stats
	}
	reportable := now.Sub(start) - overlapDuration(start, now, maintenance)
	if reportable < 0 {
		reportable = 0
	}
	var downtime time.Duration
	for _, outage := range outages {
		overlapStart, overlapEnd, ok := clippedInterval(start, now, outage)
		if !ok {
			continue
		}
		outageDuration := overlapEnd.Sub(overlapStart) - overlapDuration(overlapStart, overlapEnd, maintenance)
		if outageDuration > 0 {
			downtime += outageDuration
		}
	}
	if downtime > reportable {
		downtime = reportable
	}
	stats.ReportableSeconds = int64(reportable / time.Second)
	stats.DowntimeSeconds = int64(downtime / time.Second)
	return stats
}

func overlapDuration(start time.Time, end time.Time, intervals []timeInterval) time.Duration {
	var total time.Duration
	for _, interval := range intervals {
		overlapStart, overlapEnd, ok := clippedInterval(start, end, interval)
		if ok {
			total += overlapEnd.Sub(overlapStart)
		}
	}
	return total
}

func clippedInterval(start time.Time, end time.Time, interval timeInterval) (time.Time, time.Time, bool) {
	overlapStart := maxTime(start, interval.Start)
	overlapEnd := minTime(end, interval.End)
	if !overlapEnd.After(overlapStart) {
		return time.Time{}, time.Time{}, false
	}
	return overlapStart, overlapEnd, true
}

func minTime(a time.Time, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a time.Time, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (s *Store) ListIncidents(ctx context.Context, limit int) ([]Incident, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
		id, monitor_id, name, transition, observed_at, error, status_code
		FROM (
			SELECT id, monitor_id, name, transition, observed_at, error, status_code
			FROM incidents
			UNION ALL
			SELECT 0 AS id, probe_results.monitor_id, COALESCE(monitor_states.name, '') AS name,
				'FAILURE' AS transition, probe_results.checked_at AS observed_at, probe_results.error,
				probe_results.observed_status_code AS status_code
			FROM probe_results
			LEFT JOIN monitor_states ON monitor_states.monitor_id = probe_results.monitor_id
			WHERE probe_results.ok = 0
				AND probe_results.maintenance_window_id IS NULL
				AND probe_results.observer_suppressed = 0
				AND NOT EXISTS (
					SELECT 1 FROM incidents
					WHERE incidents.monitor_id = probe_results.monitor_id
						AND incidents.observed_at = probe_results.checked_at
				)
		)
		ORDER BY observed_at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var incidents []Incident
	for rows.Next() {
		var incident Incident
		var observed string
		if err := rows.Scan(&incident.ID, &incident.MonitorID, &incident.Name, &incident.Transition, &observed, &incident.Error, &incident.StatusCode); err != nil {
			return nil, err
		}
		incident.ObservedAt = parseTime(observed)
		incidents = append(incidents, incident)
	}
	return incidents, rows.Err()
}

func (s *Store) ListFailedProbeResults(ctx context.Context, limit int) ([]ProbeResult, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
		monitor_id, checked_at, ok, observed_status_code, latency_ms, response_time_ms,
		attempt_count, error, maintenance_window_id, observer_suppressed
		FROM probe_results WHERE ok = 0
		ORDER BY checked_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []ProbeResult
	for rows.Next() {
		result, err := scanFailedProbeResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func (s *Store) ListObserverSentinelEvents(ctx context.Context, limit int) ([]ObserverSentinelResult, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
		sentinel_id, name, url, expected_status_code, ok, observed_status_code,
		latency_ms, error, checked_at
		FROM observer_sentinel_events
		ORDER BY checked_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []ObserverSentinelResult
	for rows.Next() {
		result, err := scanObserverSentinelResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func (s *Store) ListAlertNotifications(ctx context.Context, limit int) ([]AlertNotification, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
		id, incident_id, monitor_id, provider, attempted_at, attempt_number, success, error, next_retry_at, retry_exhausted
		FROM alert_notifications
		ORDER BY attempted_at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var notifications []AlertNotification
	for rows.Next() {
		var notification AlertNotification
		var attempted string
		var nextRetry sql.NullString
		var success int
		var retryExhausted int
		if err := rows.Scan(&notification.ID, &notification.IncidentID, &notification.MonitorID, &notification.Provider, &attempted, &notification.AttemptNumber, &success, &notification.Error, &nextRetry, &retryExhausted); err != nil {
			return nil, err
		}
		notification.AttemptedAt = parseTime(attempted)
		notification.Success = intBool(success)
		if nextRetry.Valid {
			notification.NextRetryAt = parseTime(nextRetry.String)
		}
		notification.RetryExhausted = intBool(retryExhausted)
		notifications = append(notifications, notification)
	}
	return notifications, rows.Err()
}

func (s *Store) ListActionableAlertDeliveryFailures(ctx context.Context, limit int) ([]AlertNotification, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
		n.id, n.incident_id, n.monitor_id, n.provider, n.attempted_at, n.attempt_number, n.success, n.error, n.next_retry_at, n.retry_exhausted
		FROM alert_notifications n
		WHERE n.success = 0
			AND NOT EXISTS (
				SELECT 1 FROM alert_notifications newer
				WHERE newer.incident_id = n.incident_id
					AND newer.provider = n.provider
					AND newer.id > n.id
			)
		ORDER BY n.attempted_at DESC, n.id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var notifications []AlertNotification
	for rows.Next() {
		notification, err := scanAlertNotification(rows)
		if err != nil {
			return nil, err
		}
		notifications = append(notifications, notification)
	}
	return notifications, rows.Err()
}

func (s *Store) ListDueAlertNotificationRetries(ctx context.Context, now time.Time, limit int) ([]AlertNotificationRetry, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
		n.id, n.incident_id, n.monitor_id, n.provider, n.attempted_at, n.attempt_number,
		n.success, n.error, n.next_retry_at, n.retry_exhausted,
		i.id, i.monitor_id, i.name, i.transition, i.observed_at, i.error, i.status_code,
		COALESCE(s.monitor_id, i.monitor_id), COALESCE(s.name, i.name), COALESCE(s.url, ''), COALESCE(s.expected_status_code, 0),
		COALESCE(s.status, o.status, ''), COALESCE(s.status_before_maintenance, ''), COALESCE(s.consecutive_failures, o.consecutive_failures, 0),
		COALESCE(s.last_checked_at, o.last_checked_at, ''), COALESCE(s.last_success_at, o.last_success_at, ''),
		COALESCE(s.last_failure_at, o.last_failure_at, ''), COALESCE(s.last_error, o.last_error, ''),
		COALESCE(s.last_observed_status_code, 0), COALESCE(s.updated_at, o.updated_at, '')
		FROM alert_notifications n
		INNER JOIN incidents i ON i.id = n.incident_id
		LEFT JOIN monitor_states s ON s.monitor_id = n.monitor_id
		LEFT JOIN observer_state o ON n.monitor_id = '__observer__'
		WHERE n.success = 0
			AND n.retry_exhausted = 0
			AND n.next_retry_at != ''
			AND n.next_retry_at <= ?
			AND NOT EXISTS (
				SELECT 1 FROM alert_notifications newer
				WHERE newer.incident_id = n.incident_id
					AND newer.provider = n.provider
					AND newer.id > n.id
			)
			AND NOT EXISTS (
				SELECT 1 FROM alert_notifications successful
				WHERE successful.incident_id = n.incident_id
					AND successful.provider = n.provider
					AND successful.success = 1
					AND successful.id > n.id
			)
		ORDER BY n.next_retry_at ASC, n.id ASC LIMIT ?`, formatTime(now), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var retries []AlertNotificationRetry
	for rows.Next() {
		var retry AlertNotificationRetry
		var notificationAttempted string
		var notificationNextRetry sql.NullString
		var notificationSuccess, retryExhausted int
		var incidentObserved string
		var stateLastChecked, stateLastSuccess, stateLastFailure, stateUpdated string
		if err := rows.Scan(
			&retry.Notification.ID, &retry.Notification.IncidentID, &retry.Notification.MonitorID, &retry.Notification.Provider,
			&notificationAttempted, &retry.Notification.AttemptNumber, &notificationSuccess, &retry.Notification.Error,
			&notificationNextRetry, &retryExhausted,
			&retry.Incident.ID, &retry.Incident.MonitorID, &retry.Incident.Name, &retry.Incident.Transition,
			&incidentObserved, &retry.Incident.Error, &retry.Incident.StatusCode,
			&retry.CurrentState.MonitorID, &retry.CurrentState.Name, &retry.CurrentState.URL, &retry.CurrentState.ExpectedStatusCode,
			&retry.CurrentState.Status, &retry.CurrentState.StatusBeforeMaintenance, &retry.CurrentState.ConsecutiveFailures, &stateLastChecked, &stateLastSuccess,
			&stateLastFailure, &retry.CurrentState.LastError, &retry.CurrentState.LastObservedStatusCode, &stateUpdated,
		); err != nil {
			return nil, err
		}
		retry.Notification.AttemptedAt = parseTime(notificationAttempted)
		retry.Notification.Success = intBool(notificationSuccess)
		if notificationNextRetry.Valid {
			retry.Notification.NextRetryAt = parseTime(notificationNextRetry.String)
		}
		retry.Notification.RetryExhausted = intBool(retryExhausted)
		retry.Incident.ObservedAt = parseTime(incidentObserved)
		retry.CurrentState.LastCheckedAt = parseTime(stateLastChecked)
		retry.CurrentState.LastSuccessAt = parseTime(stateLastSuccess)
		retry.CurrentState.LastFailureAt = parseTime(stateLastFailure)
		retry.CurrentState.UpdatedAt = parseTime(stateUpdated)
		retries = append(retries, retry)
	}
	return retries, rows.Err()
}

func (s *Store) AddMaintenanceWindow(ctx context.Context, window MaintenanceWindow) (int64, error) {
	if !window.EndsAt.After(window.StartsAt) {
		return 0, fmt.Errorf("maintenance end must be after start")
	}
	if strings.TrimSpace(window.Reason) == "" {
		return 0, fmt.Errorf("maintenance reason is required")
	}
	if strings.TrimSpace(window.CreatedBy) == "" {
		return 0, fmt.Errorf("maintenance created_by is required")
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM monitor_states WHERE monitor_id = ?`, window.MonitorID).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("monitor %q does not exist in monitor_states", window.MonitorID)
		}
		return 0, err
	}
	var overlappingID int64
	err := s.db.QueryRowContext(ctx, `SELECT id FROM maintenance_windows
		WHERE monitor_id = ?
			AND cancelled_at IS NULL
			AND starts_at < ?
			AND ends_at > ?
		ORDER BY starts_at ASC LIMIT 1`,
		window.MonitorID, formatTime(window.EndsAt), formatTime(window.StartsAt)).Scan(&overlappingID)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	if overlappingID != 0 {
		return 0, fmt.Errorf("maintenance window overlaps existing window %d", overlappingID)
	}
	createdAt := window.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO maintenance_windows
		(monitor_id, starts_at, ends_at, reason, created_by, created_at, cancelled_at, cancelled_by, cancellation_reason)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		window.MonitorID, formatTime(window.StartsAt), formatTime(window.EndsAt), window.Reason, window.CreatedBy,
		formatTime(createdAt), nullableTime(window.CancelledAt), window.CancelledBy, window.CancellationReason)
	if err != nil {
		return 0, fmt.Errorf("insert maintenance window: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get maintenance window id: %w", err)
	}
	return id, nil
}

func (s *Store) CancelMaintenanceWindow(ctx context.Context, id int64, cancelledAt time.Time, cancelledBy string, reason string) error {
	if strings.TrimSpace(cancelledBy) == "" {
		return fmt.Errorf("maintenance cancelled_by is required")
	}
	if cancelledAt.IsZero() {
		cancelledAt = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `UPDATE maintenance_windows
		SET cancelled_at = ?, cancelled_by = ?, cancellation_reason = ?
		WHERE id = ? AND cancelled_at IS NULL`,
		formatTime(cancelledAt), cancelledBy, reason, id)
	if err != nil {
		return fmt.Errorf("cancel maintenance window: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("cancel maintenance rows affected: %w", err)
	}
	if rows == 0 {
		var existing int
		if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM maintenance_windows WHERE id = ?`, id).Scan(&existing); err == sql.ErrNoRows {
			return fmt.Errorf("maintenance window %d does not exist", id)
		} else if err != nil {
			return err
		}
		return fmt.Errorf("maintenance window %d is already cancelled", id)
	}
	return nil
}

func (s *Store) ActiveMaintenanceWindow(ctx context.Context, monitorID string, at time.Time) (MaintenanceWindow, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT
		id, monitor_id, starts_at, ends_at, reason, created_by, created_at,
		cancelled_at, cancelled_by, cancellation_reason
		FROM maintenance_windows
		WHERE monitor_id = ?
			AND cancelled_at IS NULL
			AND starts_at <= ?
			AND ends_at > ?
		ORDER BY starts_at ASC LIMIT 1`, monitorID, formatTime(at), formatTime(at))
	window, err := scanMaintenanceWindow(row)
	if err == sql.ErrNoRows {
		return MaintenanceWindow{}, false, nil
	}
	if err != nil {
		return MaintenanceWindow{}, false, err
	}
	return window, true, nil
}

func (s *Store) ListMaintenanceWindows(ctx context.Context, filter MaintenanceWindowFilter) ([]MaintenanceWindow, error) {
	query := `SELECT
		id, monitor_id, starts_at, ends_at, reason, created_by, created_at,
		cancelled_at, cancelled_by, cancellation_reason
		FROM maintenance_windows`
	var conditions []string
	var args []any
	if filter.MonitorID != "" {
		conditions = append(conditions, `monitor_id = ?`)
		args = append(args, filter.MonitorID)
	}
	if !filter.IncludeAll {
		conditions = append(conditions, `cancelled_at IS NULL`)
		if !filter.Now.IsZero() {
			conditions = append(conditions, `ends_at > ?`)
			args = append(args, formatTime(filter.Now))
		}
	}
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	query += ` ORDER BY starts_at ASC, id ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var windows []MaintenanceWindow
	for rows.Next() {
		window, err := scanMaintenanceWindow(rows)
		if err != nil {
			return nil, err
		}
		windows = append(windows, window)
	}
	return windows, rows.Err()
}

func scanAlertNotification(scanner rowScanner) (AlertNotification, error) {
	var notification AlertNotification
	var attempted string
	var nextRetry sql.NullString
	var success int
	var retryExhausted int
	if err := scanner.Scan(&notification.ID, &notification.IncidentID, &notification.MonitorID, &notification.Provider, &attempted, &notification.AttemptNumber, &success, &notification.Error, &nextRetry, &retryExhausted); err != nil {
		return AlertNotification{}, err
	}
	notification.AttemptedAt = parseTime(attempted)
	notification.Success = intBool(success)
	if nextRetry.Valid {
		notification.NextRetryAt = parseTime(nextRetry.String)
	}
	notification.RetryExhausted = intBool(retryExhausted)
	return notification, nil
}

func scanMaintenanceWindow(scanner rowScanner) (MaintenanceWindow, error) {
	var window MaintenanceWindow
	var startsAt, endsAt, createdAt string
	var cancelledAt sql.NullString
	err := scanner.Scan(
		&window.ID, &window.MonitorID, &startsAt, &endsAt, &window.Reason, &window.CreatedBy, &createdAt,
		&cancelledAt, &window.CancelledBy, &window.CancellationReason,
	)
	if err != nil {
		return MaintenanceWindow{}, err
	}
	window.StartsAt = parseTime(startsAt)
	window.EndsAt = parseTime(endsAt)
	window.CreatedAt = parseTime(createdAt)
	window.CancelledAt = parseNullTime(cancelledAt)
	return window, nil
}

func (s *Store) DeleteStatesExcept(ctx context.Context, monitorIDs []string) error {
	if len(monitorIDs) == 0 {
		_, err := s.db.ExecContext(ctx, `DELETE FROM monitor_states`)
		return err
	}

	placeholders := make([]string, len(monitorIDs))
	args := make([]any, len(monitorIDs))
	for i, id := range monitorIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM monitor_states WHERE monitor_id NOT IN (`+strings.Join(placeholders, ",")+`)`,
		args...,
	)
	return err
}

func (s *Store) PruneProbeResults(ctx context.Context, retention time.Duration, now time.Time) error {
	cutoff := now.Add(-retention)
	_, err := s.db.ExecContext(ctx, `DELETE FROM probe_results WHERE checked_at < ?`, formatTime(cutoff))
	return err
}

func (s *Store) RollupAndPruneProbeResults(ctx context.Context, policy ProbeRetentionPolicy, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if !policy.ProbeResults.Forever {
		cutoff := now.UTC().Add(-policy.ProbeResults.Duration)
		if err := compactRawOutcomeRuns(ctx, tx, cutoff); err != nil {
			return err
		}
		if err := rollupRawProbeResults(ctx, tx, "probe_minute_rollups", minuteBucketExpression(), cutoff); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM probe_results WHERE checked_at < ?`, formatTime(cutoff)); err != nil {
			return err
		}
	}
	if !policy.ProbeMinuteRollups.Forever {
		cutoff := now.UTC().Add(-policy.ProbeMinuteRollups.Duration)
		if err := rollupStoredProbeRollups(ctx, tx, "probe_minute_rollups", "probe_hourly_rollups", hourlyBucketExpression(), cutoff); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM probe_minute_rollups WHERE bucket_start < ?`, formatTime(cutoff)); err != nil {
			return err
		}
	}
	if !policy.ProbeHourlyRollups.Forever {
		cutoff := now.UTC().Add(-policy.ProbeHourlyRollups.Duration)
		if err := rollupStoredProbeRollups(ctx, tx, "probe_hourly_rollups", "probe_daily_rollups", dailyBucketExpression(), cutoff); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM probe_hourly_rollups WHERE bucket_start < ?`, formatTime(cutoff)); err != nil {
			return err
		}
	}
	if !policy.ProbeDailyRollups.Forever {
		cutoff := now.UTC().Add(-policy.ProbeDailyRollups.Duration)
		if _, err := tx.ExecContext(ctx, `DELETE FROM probe_daily_rollups WHERE bucket_start < ?`, formatTime(cutoff)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM probe_outcome_runs WHERE ended_at < ?`, formatTime(cutoff)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func compactRawOutcomeRuns(ctx context.Context, tx *sql.Tx, cutoff time.Time) error {
	rows, err := tx.QueryContext(ctx, `SELECT monitor_id, checked_at, ok
		FROM probe_results
		WHERE checked_at < ?
			AND maintenance_window_id IS NULL
			AND observer_suppressed = 0
		ORDER BY monitor_id ASC, checked_at ASC, id ASC`, formatTime(cutoff))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var monitorID string
		var checkedAtRaw string
		var okInt int
		if err := rows.Scan(&monitorID, &checkedAtRaw, &okInt); err != nil {
			return err
		}
		if err := appendOutcomeRun(ctx, tx, monitorID, parseTime(checkedAtRaw), intBool(okInt)); err != nil {
			return err
		}
	}
	return rows.Err()
}

func appendOutcomeRun(ctx context.Context, tx *sql.Tx, monitorID string, checkedAt time.Time, ok bool) error {
	var id int64
	var lastOK int
	err := tx.QueryRowContext(ctx, `SELECT id, ok
		FROM probe_outcome_runs
		WHERE monitor_id = ?
		ORDER BY started_at DESC, id DESC LIMIT 1`, monitorID).Scan(&id, &lastOK)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == nil && intBool(lastOK) == ok {
		_, err = tx.ExecContext(ctx, `UPDATE probe_outcome_runs
			SET ended_at = ?, probe_count = probe_count + 1
			WHERE id = ?`, formatTime(checkedAt), id)
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO probe_outcome_runs
		(monitor_id, started_at, ended_at, ok, probe_count)
		VALUES (?, ?, ?, ?, 1)`, monitorID, formatTime(checkedAt), formatTime(checkedAt), boolInt(ok))
	return err
}

func rollupRawProbeResults(ctx context.Context, tx *sql.Tx, target string, bucketExpr string, cutoff time.Time) error {
	query := `INSERT INTO ` + target + `
		(monitor_id, bucket_start, total_checks, successful_checks, maintenance_checks, maintenance_failed_checks,
			observer_suppressed_checks, first_reportable_at, last_reportable_at)
		SELECT monitor_id, ` + bucketExpr + ` AS bucket_start,
			COALESCE(SUM(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = 0 THEN ok ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN maintenance_window_id IS NOT NULL THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN maintenance_window_id IS NOT NULL AND ok = 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN observer_suppressed = 1 THEN 1 ELSE 0 END), 0),
			MIN(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = 0 THEN checked_at ELSE NULL END),
			MAX(CASE WHEN maintenance_window_id IS NULL AND observer_suppressed = 0 THEN checked_at ELSE NULL END)
		FROM probe_results
		WHERE checked_at < ?
		GROUP BY monitor_id, 2
		ON CONFLICT(monitor_id, bucket_start) DO UPDATE SET
			total_checks = ` + target + `.total_checks + excluded.total_checks,
			successful_checks = ` + target + `.successful_checks + excluded.successful_checks,
			maintenance_checks = ` + target + `.maintenance_checks + excluded.maintenance_checks,
			maintenance_failed_checks = ` + target + `.maintenance_failed_checks + excluded.maintenance_failed_checks,
			observer_suppressed_checks = ` + target + `.observer_suppressed_checks + excluded.observer_suppressed_checks,
			first_reportable_at = ` + mergedFirstReportableAt(target+`.first_reportable_at`, `excluded.first_reportable_at`) + `,
			last_reportable_at = ` + mergedLastReportableAt(target+`.last_reportable_at`, `excluded.last_reportable_at`)
	_, err := tx.ExecContext(ctx, query, formatTime(cutoff))
	return err
}

func rollupStoredProbeRollups(ctx context.Context, tx *sql.Tx, source string, target string, bucketExpr string, cutoff time.Time) error {
	query := `INSERT INTO ` + target + `
		(monitor_id, bucket_start, total_checks, successful_checks, maintenance_checks, maintenance_failed_checks,
			observer_suppressed_checks, first_reportable_at, last_reportable_at)
		SELECT monitor_id, ` + bucketExpr + ` AS bucket_start,
			COALESCE(SUM(total_checks), 0),
			COALESCE(SUM(successful_checks), 0),
			COALESCE(SUM(maintenance_checks), 0),
			COALESCE(SUM(maintenance_failed_checks), 0),
			COALESCE(SUM(observer_suppressed_checks), 0),
			MIN(first_reportable_at),
			MAX(last_reportable_at)
		FROM ` + source + `
		WHERE bucket_start < ?
		GROUP BY monitor_id, 2
		ON CONFLICT(monitor_id, bucket_start) DO UPDATE SET
			total_checks = ` + target + `.total_checks + excluded.total_checks,
			successful_checks = ` + target + `.successful_checks + excluded.successful_checks,
			maintenance_checks = ` + target + `.maintenance_checks + excluded.maintenance_checks,
			maintenance_failed_checks = ` + target + `.maintenance_failed_checks + excluded.maintenance_failed_checks,
			observer_suppressed_checks = ` + target + `.observer_suppressed_checks + excluded.observer_suppressed_checks,
			first_reportable_at = ` + mergedFirstReportableAt(target+`.first_reportable_at`, `excluded.first_reportable_at`) + `,
			last_reportable_at = ` + mergedLastReportableAt(target+`.last_reportable_at`, `excluded.last_reportable_at`)
	_, err := tx.ExecContext(ctx, query, formatTime(cutoff))
	return err
}

func mergedFirstReportableAt(existing string, incoming string) string {
	return `CASE
		WHEN ` + existing + ` IS NULL OR ` + existing + ` = '' THEN ` + incoming + `
		WHEN ` + incoming + ` IS NULL OR ` + incoming + ` = '' THEN ` + existing + `
		WHEN ` + incoming + ` < ` + existing + ` THEN ` + incoming + `
		ELSE ` + existing + `
	END`
}

func mergedLastReportableAt(existing string, incoming string) string {
	return `CASE
		WHEN ` + existing + ` IS NULL OR ` + existing + ` = '' THEN ` + incoming + `
		WHEN ` + incoming + ` IS NULL OR ` + incoming + ` = '' THEN ` + existing + `
		WHEN ` + incoming + ` > ` + existing + ` THEN ` + incoming + `
		ELSE ` + existing + `
	END`
}

func minuteBucketExpression() string {
	return `substr(checked_at, 1, 17) || '00Z'`
}

func hourlyBucketExpression() string {
	return `substr(bucket_start, 1, 14) || '00:00Z'`
}

func dailyBucketExpression() string {
	return `substr(bucket_start, 1, 11) || '00:00:00Z'`
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanState(scanner rowScanner) (MonitorState, error) {
	var state MonitorState
	var lastChecked, lastSuccess, lastFailure, updated string
	err := scanner.Scan(
		&state.MonitorID, &state.Name, &state.URL, &state.ExpectedStatusCode, &state.Status, &state.StatusBeforeMaintenance, &state.ConsecutiveFailures,
		&lastChecked, &lastSuccess, &lastFailure, &state.LastError, &state.LastObservedStatusCode, &updated,
	)
	if err != nil {
		return MonitorState{}, err
	}
	state.LastCheckedAt = parseTime(lastChecked)
	state.LastSuccessAt = parseTime(lastSuccess)
	state.LastFailureAt = parseTime(lastFailure)
	state.UpdatedAt = parseTime(updated)
	return state, nil
}

func scanObserverState(scanner rowScanner) (ObserverState, error) {
	var state ObserverState
	var lastChecked, lastSuccess, lastFailure, updated string
	err := scanner.Scan(
		&state.Status, &state.ConsecutiveFailures, &state.ConsecutiveSuccesses,
		&lastChecked, &lastSuccess, &lastFailure, &state.LastError, &updated,
	)
	if err != nil {
		return ObserverState{}, err
	}
	state.LastCheckedAt = parseTime(lastChecked)
	state.LastSuccessAt = parseTime(lastSuccess)
	state.LastFailureAt = parseTime(lastFailure)
	state.UpdatedAt = parseTime(updated)
	return state, nil
}

func scanObserverSentinelResult(scanner rowScanner) (ObserverSentinelResult, error) {
	var result ObserverSentinelResult
	var ok int
	var checkedAt string
	err := scanner.Scan(
		&result.SentinelID, &result.Name, &result.URL, &result.ExpectedStatusCode,
		&ok, &result.ObservedStatusCode, &result.LatencyMS, &result.Error, &checkedAt,
	)
	if err != nil {
		return ObserverSentinelResult{}, err
	}
	result.OK = intBool(ok)
	result.CheckedAt = parseTime(checkedAt)
	return result, nil
}

func scanFailedProbeResult(scanner rowScanner) (ProbeResult, error) {
	var result ProbeResult
	var checkedAt string
	var ok int
	var maintenanceWindowID sql.NullInt64
	var observerSuppressed int
	err := scanner.Scan(
		&result.MonitorID, &checkedAt, &ok, &result.ObservedStatusCode,
		&result.LatencyMS, &result.ResponseTimeMS, &result.AttemptCount,
		&result.Error, &maintenanceWindowID, &observerSuppressed,
	)
	if err != nil {
		return ProbeResult{}, err
	}
	result.CheckedAt = parseTime(checkedAt)
	result.OK = intBool(ok)
	if maintenanceWindowID.Valid {
		result.MaintenanceWindowID = maintenanceWindowID.Int64
	}
	result.ObserverSuppressed = intBool(observerSuppressed)
	return result, nil
}

func scanStatusInterval(scanner rowScanner) (StatusInterval, error) {
	var interval StatusInterval
	var startedAt string
	var endedAt sql.NullString
	var downtime int
	if err := scanner.Scan(&interval.ID, &interval.MonitorID, &interval.Status, &startedAt, &endedAt, &downtime); err != nil {
		return StatusInterval{}, err
	}
	interval.StartedAt = parseTime(startedAt)
	interval.EndedAt = parseNullTime(endedAt)
	interval.Downtime = intBool(downtime)
	return interval, nil
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}
	}
	return t
}

func parseNullTime(raw sql.NullString) time.Time {
	if !raw.Valid {
		return time.Time{}
	}
	return parseTime(raw.String)
}

func nullableInt64(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return formatTime(value)
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func intBool(value int) bool {
	return value != 0
}
