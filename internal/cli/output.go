package cli

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"upag/internal/storage"
)

func PrintStates(w io.Writer, states []storage.MonitorState, activeMaintenance map[string]storage.MaintenanceWindow) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ID\tSTATUS\tFAILURES\tLAST CHECK\tLAST FAILED\tLAST STATUS\tMAINTENANCE\tMAINTENANCE UNTIL\tNAME\tURL\tERROR"); err != nil {
		return err
	}
	for _, state := range states {
		maintenanceID := "-"
		maintenanceUntil := "-"
		if window, ok := activeMaintenance[state.MonitorID]; ok {
			maintenanceID = fmt.Sprintf("%d", window.ID)
			maintenanceUntil = formatCLITime(window.EndsAt)
		}
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
			state.MonitorID,
			state.Status,
			state.ConsecutiveFailures,
			formatCLITime(state.LastCheckedAt),
			formatCLITime(state.LastFailureAt),
			state.LastObservedStatusCode,
			maintenanceID,
			maintenanceUntil,
			state.Name,
			state.URL,
			state.LastError,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func PrintMaintenanceWindows(w io.Writer, windows []storage.MaintenanceWindow, now time.Time) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ID\tMONITOR\tSTATE\tSTART\tEND\tCREATED BY\tCREATED AT\tCANCELLED BY\tCANCELLED AT\tREASON\tCANCELLATION REASON"); err != nil {
		return err
	}
	for _, window := range windows {
		if _, err := fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			window.ID,
			window.MonitorID,
			maintenanceWindowState(window, now),
			formatCLITime(window.StartsAt),
			formatCLITime(window.EndsAt),
			window.CreatedBy,
			formatCLITime(window.CreatedAt),
			emptyDash(window.CancelledBy),
			formatCLITime(window.CancelledAt),
			window.Reason,
			emptyDash(window.CancellationReason),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func PrintIncidents(w io.Writer, incidents []storage.Incident) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TIME\tEVENT\tSTATUS_CODE\tMONITOR\tNAME\tERROR"); err != nil {
		return err
	}
	for _, incident := range incidents {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\n",
			formatCLITime(incident.ObservedAt),
			incident.Transition,
			incident.StatusCode,
			incident.MonitorID,
			incident.Name,
			incident.Error,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func maintenanceWindowState(window storage.MaintenanceWindow, now time.Time) string {
	if !window.CancelledAt.IsZero() {
		return "CANCELLED"
	}
	if !now.Before(window.StartsAt) && now.Before(window.EndsAt) {
		return "ACTIVE"
	}
	if now.Before(window.StartsAt) {
		return "UPCOMING"
	}
	return "ENDED"
}

func emptyDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
