package cli

import (
	"fmt"
	"io"
	"text/tabwriter"

	"upag/internal/storage"
)

func PrintStates(w io.Writer, states []storage.MonitorState) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ID\tSTATUS\tFAILURES\tLAST CHECK\tLAST STATUS\tNAME\tURL\tERROR"); err != nil {
		return err
	}
	for _, state := range states {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%d\t%s\t%s\t%s\n",
			state.MonitorID,
			state.Status,
			state.ConsecutiveFailures,
			formatCLITime(state.LastCheckedAt),
			state.LastObservedStatusCode,
			state.Name,
			state.URL,
			state.LastError,
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
