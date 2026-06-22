package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Store struct {
	db *sql.DB
}

type MonitorState struct {
	MonitorID              string
	Name                   string
	URL                    string
	ExpectedStatusCode     int
	Status                 string
	ConsecutiveFailures    int
	LastCheckedAt          time.Time
	LastSuccessAt          time.Time
	LastFailureAt          time.Time
	LastError              string
	LastObservedStatusCode int
	UpdatedAt              time.Time
}

type ProbeResult struct {
	MonitorID          string
	CheckedAt          time.Time
	OK                 bool
	ObservedStatusCode int
	LatencyMS          int64
	ResponseTimeMS     int64
	Error              string
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

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate(ctx context.Context) error {
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
			response_time_ms INTEGER NOT NULL DEFAULT 0,
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
			attempt_number INTEGER NOT NULL DEFAULT 1,
			success INTEGER NOT NULL,
			error TEXT NOT NULL,
			next_retry_at TEXT,
			retry_exhausted INTEGER NOT NULL DEFAULT 0,
			FOREIGN KEY (incident_id) REFERENCES incidents(id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_alert_notifications_incident
			ON alert_notifications (incident_id)`,
		`CREATE INDEX IF NOT EXISTS idx_alert_notifications_monitor_attempted
			ON alert_notifications (monitor_id, attempted_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_alert_notifications_attempted
			ON alert_notifications (attempted_at DESC)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate sqlite database: %w", err)
		}
	}
	if err := s.ensureAlertNotificationColumns(ctx); err != nil {
		return err
	}
	if err := s.ensureProbeResultColumns(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureProbeResultColumns(ctx context.Context) error {
	columns, err := s.tableColumns(ctx, "probe_results")
	if err != nil {
		return err
	}
	alterStatements := map[string]string{
		"response_time_ms": `ALTER TABLE probe_results ADD COLUMN response_time_ms INTEGER NOT NULL DEFAULT 0`,
	}
	for column, statement := range alterStatements {
		if !columns[column] {
			if _, err := s.db.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("migrate probe_results.%s: %w", column, err)
			}
		}
	}
	return nil
}

func (s *Store) ensureAlertNotificationColumns(ctx context.Context) error {
	columns, err := s.tableColumns(ctx, "alert_notifications")
	if err != nil {
		return err
	}
	alterStatements := map[string]string{
		"attempt_number":  `ALTER TABLE alert_notifications ADD COLUMN attempt_number INTEGER NOT NULL DEFAULT 1`,
		"next_retry_at":   `ALTER TABLE alert_notifications ADD COLUMN next_retry_at TEXT`,
		"retry_exhausted": `ALTER TABLE alert_notifications ADD COLUMN retry_exhausted INTEGER NOT NULL DEFAULT 0`,
	}
	for column, statement := range alterStatements {
		if !columns[column] {
			if _, err := s.db.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("migrate alert_notifications.%s: %w", column, err)
			}
		}
	}
	return nil
}

func (s *Store) tableColumns(ctx context.Context, table string) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
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
		monitor_id, name, url, expected_status_code, status, consecutive_failures,
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

	if _, err := tx.ExecContext(ctx, `INSERT INTO probe_results
		(monitor_id, checked_at, ok, observed_status_code, latency_ms, response_time_ms, error)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		result.MonitorID, formatTime(result.CheckedAt), boolInt(result.OK), result.ObservedStatusCode, result.LatencyMS, result.ResponseTimeMS, result.Error); err != nil {
		return 0, fmt.Errorf("insert probe result: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO monitor_states
		(monitor_id, name, url, expected_status_code, status, consecutive_failures,
		last_checked_at, last_success_at, last_failure_at, last_error,
		last_observed_status_code, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(monitor_id) DO UPDATE SET
			name = excluded.name,
			url = excluded.url,
			expected_status_code = excluded.expected_status_code,
			status = excluded.status,
			consecutive_failures = excluded.consecutive_failures,
			last_checked_at = excluded.last_checked_at,
			last_success_at = excluded.last_success_at,
			last_failure_at = excluded.last_failure_at,
			last_error = excluded.last_error,
			last_observed_status_code = excluded.last_observed_status_code,
			updated_at = excluded.updated_at`,
		next.MonitorID, next.Name, next.URL, next.ExpectedStatusCode, next.Status, next.ConsecutiveFailures,
		formatTime(next.LastCheckedAt), formatTime(next.LastSuccessAt), formatTime(next.LastFailureAt), next.LastError,
		next.LastObservedStatusCode, formatTime(next.UpdatedAt)); err != nil {
		return 0, fmt.Errorf("save monitor state: %w", err)
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
		monitor_id, name, url, expected_status_code, status, consecutive_failures,
		last_checked_at, last_success_at, last_failure_at, last_error,
		last_observed_status_code, updated_at
		FROM monitor_states ORDER BY monitor_id`)
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
		s.monitor_id, s.name, s.url, s.expected_status_code, s.status, s.consecutive_failures,
		s.last_checked_at, s.last_success_at, s.last_failure_at, s.last_error,
		s.last_observed_status_code, s.updated_at
		FROM alert_notifications n
		INNER JOIN incidents i ON i.id = n.incident_id
		INNER JOIN monitor_states s ON s.monitor_id = n.monitor_id
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
			&retry.CurrentState.Status, &retry.CurrentState.ConsecutiveFailures, &stateLastChecked, &stateLastSuccess,
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

type rowScanner interface {
	Scan(dest ...any) error
}

func scanState(scanner rowScanner) (MonitorState, error) {
	var state MonitorState
	var lastChecked, lastSuccess, lastFailure, updated string
	err := scanner.Scan(
		&state.MonitorID, &state.Name, &state.URL, &state.ExpectedStatusCode, &state.Status, &state.ConsecutiveFailures,
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

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func intBool(value int) bool {
	return value != 0
}
