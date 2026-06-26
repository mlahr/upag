package storage

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

const maxMigrationTenantIDLength = 63

var migrationTenantIDPattern = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9._-]{0,61}[a-zA-Z0-9])?$`)

type MigrationLogger func(format string, args ...any)

type migrationOptions struct {
	logger MigrationLogger
}

type MigrationOption func(*migrationOptions)

func WithMigrationLogger(logger MigrationLogger) MigrationOption {
	return func(opts *migrationOptions) {
		opts.logger = logger
	}
}

func MigrateSQLiteToPostgres(ctx context.Context, sqlitePath string, postgresDSN string, tenantID string, opts ...MigrationOption) error {
	options := migrationOptions{
		logger: func(string, ...any) {},
	}
	for _, apply := range opts {
		apply(&options)
	}

	tenantID, err := normalizeMigrationTenantID(tenantID)
	if err != nil {
		return err
	}
	options.logger("starting migration tenant=%q source=%q", tenantID, sqlitePath)
	source, err := Open(sqlitePath)
	if err != nil {
		return err
	}
	defer source.Close()
	options.logger("sqlite source opened")

	target, err := OpenPostgres(ctx, postgresDSN)
	if err != nil {
		return err
	}
	defer target.Close()
	options.logger("postgres target opened")

	if err := ensurePostgresTargetEmpty(ctx, target); err != nil {
		return err
	}
	options.logger("target tables confirmed empty")

	tx, err := target.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	options.logger("copying sqlite rows into tenant=%q", tenantID)
	if err := copyMonitorStates(ctx, source.db, tx, tenantID); err != nil {
		return err
	}
	if err := copyObserverState(ctx, source.db, tx, tenantID); err != nil {
		return err
	}
	if err := copyObserverSentinelResults(ctx, source.db, tx, tenantID); err != nil {
		return err
	}
	if err := copyMaintenanceWindows(ctx, source.db, tx, tenantID); err != nil {
		return err
	}
	if err := copyProbeResults(ctx, source.db, tx, tenantID); err != nil {
		return err
	}
	if err := copyIncidents(ctx, source.db, tx, tenantID); err != nil {
		return err
	}
	if err := copyAlertNotifications(ctx, source.db, tx, tenantID); err != nil {
		return err
	}
	if err := copyProbeRollups(ctx, source.db, tx, tenantID, "probe_minute_rollups"); err != nil {
		return err
	}
	if err := copyProbeRollups(ctx, source.db, tx, tenantID, "probe_hourly_rollups"); err != nil {
		return err
	}
	if err := copyProbeRollups(ctx, source.db, tx, tenantID, "probe_daily_rollups"); err != nil {
		return err
	}
	if err := copyProbeOutcomeRuns(ctx, source.db, tx, tenantID); err != nil {
		return err
	}
	options.logger("sqlite row copy complete for tenant=%q", tenantID)
	if err := resetPostgresSequences(ctx, tx); err != nil {
		return err
	}
	options.logger("reset postgres sequences")
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	options.logger("migration committed successfully for tenant=%q", tenantID)
	return nil
}

func normalizeMigrationTenantID(raw string) (string, error) {
	tenantID := strings.TrimSpace(raw)
	if tenantID == "" {
		return "", fmt.Errorf("migration tenant_id is required")
	}
	if tenantID != raw {
		return "", fmt.Errorf("migration tenant_id must not have leading or trailing whitespace")
	}
	if len(tenantID) > maxMigrationTenantIDLength {
		return "", fmt.Errorf("migration tenant_id must not exceed %d characters", maxMigrationTenantIDLength)
	}
	if !migrationTenantIDPattern.MatchString(tenantID) {
		return "", fmt.Errorf("migration tenant_id must match %s", migrationTenantIDPattern.String())
	}
	return tenantID, nil
}

func ensurePostgresTargetEmpty(ctx context.Context, target *PostgresStore) error {
	for _, table := range []string{
		"monitor_states",
		"probe_results",
		"incidents",
		"alert_notifications",
		"maintenance_windows",
		"observer_state",
		"observer_sentinel_results",
		"probe_minute_rollups",
		"probe_hourly_rollups",
		"probe_daily_rollups",
		"probe_outcome_runs",
	} {
		var count int
		if err := target.pool.QueryRow(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("postgres target table %s is not empty", table)
		}
	}
	return nil
}

func copyMonitorStates(ctx context.Context, db *sql.DB, tx postgresTx, tenantID string) error {
	rows, err := db.QueryContext(ctx, `SELECT monitor_id, name, url, expected_status_code, status, status_before_maintenance,
		consecutive_failures, last_checked_at, last_success_at, last_failure_at, last_error, last_observed_status_code, updated_at
		FROM monitor_states ORDER BY monitor_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var state MonitorState
		var lastChecked, lastSuccess, lastFailure, updated string
		if err := rows.Scan(&state.MonitorID, &state.Name, &state.URL, &state.ExpectedStatusCode, &state.Status, &state.StatusBeforeMaintenance, &state.ConsecutiveFailures, &lastChecked, &lastSuccess, &lastFailure, &state.LastError, &state.LastObservedStatusCode, &updated); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO monitor_states
			(tenant_id, monitor_id, name, url, expected_status_code, status, status_before_maintenance, consecutive_failures,
			 last_checked_at, last_success_at, last_failure_at, last_error, last_observed_status_code, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
			tenantID, state.MonitorID, state.Name, state.URL, state.ExpectedStatusCode, state.Status, state.StatusBeforeMaintenance, state.ConsecutiveFailures,
			postgresNullableTime(parseTime(lastChecked)), postgresNullableTime(parseTime(lastSuccess)), postgresNullableTime(parseTime(lastFailure)), state.LastError,
			state.LastObservedStatusCode, parseTime(updated)); err != nil {
			return err
		}
	}
	return rows.Err()
}

func copyObserverState(ctx context.Context, db *sql.DB, tx postgresTx, tenantID string) error {
	rows, err := db.QueryContext(ctx, `SELECT status, consecutive_failures, consecutive_successes, last_checked_at, last_success_at, last_failure_at, last_error, updated_at FROM observer_state WHERE id = 1`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var state ObserverState
		var lastChecked, lastSuccess, lastFailure, updated string
		if err := rows.Scan(&state.Status, &state.ConsecutiveFailures, &state.ConsecutiveSuccesses, &lastChecked, &lastSuccess, &lastFailure, &state.LastError, &updated); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO observer_state
			(tenant_id, id, status, consecutive_failures, consecutive_successes, last_checked_at, last_success_at, last_failure_at, last_error, updated_at)
			VALUES ($1, 1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			tenantID, state.Status, state.ConsecutiveFailures, state.ConsecutiveSuccesses, postgresNullableTime(parseTime(lastChecked)),
			postgresNullableTime(parseTime(lastSuccess)), postgresNullableTime(parseTime(lastFailure)), state.LastError, parseTime(updated)); err != nil {
			return err
		}
	}
	return rows.Err()
}

func copyObserverSentinelResults(ctx context.Context, db *sql.DB, tx postgresTx, tenantID string) error {
	rows, err := db.QueryContext(ctx, `SELECT sentinel_id, name, url, expected_status_code, ok, observed_status_code, latency_ms, error, checked_at FROM observer_sentinel_results ORDER BY sentinel_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var result ObserverSentinelResult
		var ok int
		var checkedAt string
		if err := rows.Scan(&result.SentinelID, &result.Name, &result.URL, &result.ExpectedStatusCode, &ok, &result.ObservedStatusCode, &result.LatencyMS, &result.Error, &checkedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO observer_sentinel_results
			(tenant_id, sentinel_id, name, url, expected_status_code, ok, observed_status_code, latency_ms, error, checked_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			tenantID, result.SentinelID, result.Name, result.URL, result.ExpectedStatusCode, intBool(ok), result.ObservedStatusCode, result.LatencyMS, result.Error, parseTime(checkedAt)); err != nil {
			return err
		}
	}
	return rows.Err()
}

func copyMaintenanceWindows(ctx context.Context, db *sql.DB, tx postgresTx, tenantID string) error {
	rows, err := db.QueryContext(ctx, `SELECT id, monitor_id, starts_at, ends_at, reason, created_by, created_at, cancelled_at, cancelled_by, cancellation_reason FROM maintenance_windows ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var window MaintenanceWindow
		var startsAt, endsAt, createdAt string
		var cancelledAt sql.NullString
		if err := rows.Scan(&window.ID, &window.MonitorID, &startsAt, &endsAt, &window.Reason, &window.CreatedBy, &createdAt, &cancelledAt, &window.CancelledBy, &window.CancellationReason); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO maintenance_windows
			(tenant_id, id, monitor_id, starts_at, ends_at, reason, created_by, created_at, cancelled_at, cancelled_by, cancellation_reason)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			tenantID, window.ID, window.MonitorID, parseTime(startsAt), parseTime(endsAt), window.Reason, window.CreatedBy, parseTime(createdAt), postgresNullableTime(parseNullTime(cancelledAt)), window.CancelledBy, window.CancellationReason); err != nil {
			return err
		}
	}
	return rows.Err()
}

func copyProbeResults(ctx context.Context, db *sql.DB, tx postgresTx, tenantID string) error {
	rows, err := db.QueryContext(ctx, `SELECT id, monitor_id, checked_at, ok, observed_status_code, latency_ms, response_time_ms, attempt_count, error, maintenance_window_id, observer_suppressed FROM probe_results ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var result ProbeResult
		var checkedAt string
		var ok, suppressed int
		var maintenanceID sql.NullInt64
		if err := rows.Scan(&id, &result.MonitorID, &checkedAt, &ok, &result.ObservedStatusCode, &result.LatencyMS, &result.ResponseTimeMS, &result.AttemptCount, &result.Error, &maintenanceID, &suppressed); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO probe_results
			(tenant_id, id, monitor_id, checked_at, ok, observed_status_code, latency_ms, response_time_ms, attempt_count, error, maintenance_window_id, observer_suppressed)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			tenantID, id, result.MonitorID, parseTime(checkedAt), intBool(ok), result.ObservedStatusCode, result.LatencyMS, result.ResponseTimeMS, result.AttemptCount, result.Error, nullInt64Value(maintenanceID), intBool(suppressed)); err != nil {
			return err
		}
	}
	return rows.Err()
}

func copyIncidents(ctx context.Context, db *sql.DB, tx postgresTx, tenantID string) error {
	rows, err := db.QueryContext(ctx, `SELECT id, monitor_id, name, transition, observed_at, error, status_code FROM incidents ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var incident Incident
		var observedAt string
		if err := rows.Scan(&incident.ID, &incident.MonitorID, &incident.Name, &incident.Transition, &observedAt, &incident.Error, &incident.StatusCode); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO incidents
			(tenant_id, id, monitor_id, name, transition, observed_at, error, status_code)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			tenantID, incident.ID, incident.MonitorID, incident.Name, incident.Transition, parseTime(observedAt), incident.Error, incident.StatusCode); err != nil {
			return err
		}
	}
	return rows.Err()
}

func copyAlertNotifications(ctx context.Context, db *sql.DB, tx postgresTx, tenantID string) error {
	rows, err := db.QueryContext(ctx, `SELECT id, incident_id, monitor_id, provider, attempted_at, attempt_number, success, error, next_retry_at, retry_exhausted FROM alert_notifications ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var notification AlertNotification
		var attemptedAt string
		var nextRetry sql.NullString
		var success, exhausted int
		if err := rows.Scan(&notification.ID, &notification.IncidentID, &notification.MonitorID, &notification.Provider, &attemptedAt, &notification.AttemptNumber, &success, &notification.Error, &nextRetry, &exhausted); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO alert_notifications
			(tenant_id, id, incident_id, monitor_id, provider, attempted_at, attempt_number, success, error, next_retry_at, retry_exhausted)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			tenantID, notification.ID, notification.IncidentID, notification.MonitorID, notification.Provider, parseTime(attemptedAt), notification.AttemptNumber, intBool(success), notification.Error, postgresNullableTime(parseNullTime(nextRetry)), intBool(exhausted)); err != nil {
			return err
		}
	}
	return rows.Err()
}

func copyProbeRollups(ctx context.Context, db *sql.DB, tx postgresTx, tenantID string, table string) error {
	rows, err := db.QueryContext(ctx, `SELECT monitor_id, bucket_start, total_checks, successful_checks, maintenance_checks, maintenance_failed_checks, observer_suppressed_checks, first_reportable_at, last_reportable_at FROM `+table+` ORDER BY monitor_id, bucket_start`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var monitorID, bucketStart string
		var total, successful, maintenance, maintenanceFailed, suppressed int
		var first, last sql.NullString
		if err := rows.Scan(&monitorID, &bucketStart, &total, &successful, &maintenance, &maintenanceFailed, &suppressed, &first, &last); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+table+`
			(tenant_id, monitor_id, bucket_start, total_checks, successful_checks, maintenance_checks, maintenance_failed_checks, observer_suppressed_checks, first_reportable_at, last_reportable_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			tenantID, monitorID, parseTime(bucketStart), total, successful, maintenance, maintenanceFailed, suppressed, postgresNullableTime(parseNullTime(first)), postgresNullableTime(parseNullTime(last))); err != nil {
			return err
		}
	}
	return rows.Err()
}

func copyProbeOutcomeRuns(ctx context.Context, db *sql.DB, tx postgresTx, tenantID string) error {
	rows, err := db.QueryContext(ctx, `SELECT id, monitor_id, started_at, ended_at, ok, probe_count FROM probe_outcome_runs ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var monitorID, startedAt, endedAt string
		var ok int
		var probeCount int
		if err := rows.Scan(&id, &monitorID, &startedAt, &endedAt, &ok, &probeCount); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO probe_outcome_runs
			(tenant_id, id, monitor_id, started_at, ended_at, ok, probe_count)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			tenantID, id, monitorID, parseTime(startedAt), parseTime(endedAt), intBool(ok), probeCount); err != nil {
			return err
		}
	}
	return rows.Err()
}

func resetPostgresSequences(ctx context.Context, tx postgresTx) error {
	for _, table := range []string{"probe_results", "incidents", "alert_notifications", "maintenance_windows", "probe_outcome_runs"} {
		if _, err := tx.Exec(ctx, `SELECT setval(pg_get_serial_sequence($1, 'id'), COALESCE((SELECT MAX(id) FROM `+table+`), 1), (SELECT COUNT(*) > 0 FROM `+table+`))`, table); err != nil {
			return err
		}
	}
	return nil
}

type postgresTx interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func nullInt64Value(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}
