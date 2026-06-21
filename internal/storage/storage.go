package storage

import (
	"context"
	"database/sql"
	"fmt"
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
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate sqlite database: %w", err)
		}
	}
	return nil
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

func (s *Store) SaveProbeAndState(ctx context.Context, result ProbeResult, next MonitorState, incident *Incident) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `INSERT INTO probe_results
		(monitor_id, checked_at, ok, observed_status_code, latency_ms, error)
		VALUES (?, ?, ?, ?, ?, ?)`,
		result.MonitorID, formatTime(result.CheckedAt), boolInt(result.OK), result.ObservedStatusCode, result.LatencyMS, result.Error); err != nil {
		return fmt.Errorf("insert probe result: %w", err)
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
		return fmt.Errorf("save monitor state: %w", err)
	}

	if incident != nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO incidents
			(monitor_id, name, transition, observed_at, error, status_code)
			VALUES (?, ?, ?, ?, ?, ?)`,
			incident.MonitorID, incident.Name, incident.Transition, formatTime(incident.ObservedAt), incident.Error, incident.StatusCode); err != nil {
			return fmt.Errorf("insert incident: %w", err)
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
		FROM incidents ORDER BY observed_at DESC, id DESC LIMIT ?`, limit)
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
