package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"upag/internal/controlapi"
	"upag/internal/storage"
)

func PrintStates(w io.Writer, states []storage.MonitorState, activeMaintenance map[string]storage.MaintenanceWindow) error {
	return printTable(w, func(tw *tabwriter.Writer) error {
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
		return nil
	})
}

func PrintUptimeAt(w io.Writer, monitors []storage.MonitorUptime, generatedAt time.Time) error {
	return printTable(w, func(tw *tabwriter.Writer) error {
		if _, err := fmt.Fprintln(tw, "ID\tSTATUS\tUPTIME SINCE LAST FAILURE\tUPTIME SINCE LAST DOWNTIME\tNAME"); err != nil {
			return err
		}
		for _, monitor := range monitors {
			if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
				monitor.MonitorID,
				monitor.Status,
				formatElapsedSince(monitor.FailureFreeSince, generatedAt),
				formatElapsedSince(monitor.DowntimeFreeSince, generatedAt),
				monitor.Name,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func formatElapsedSince(event, now time.Time) string {
	if event.IsZero() || event.After(now) {
		return "-"
	}
	event = event.UTC()
	now = now.UTC()

	totalMonths := (now.Year()-event.Year())*12 + int(now.Month()-event.Month())
	for totalMonths > 0 && addCalendarMonthsClamped(event, totalMonths).After(now) {
		totalMonths--
	}
	cursor := addCalendarMonthsClamped(event, totalMonths)
	remainder := now.Sub(cursor)

	years := totalMonths / 12
	months := totalMonths % 12
	days := int(remainder / (24 * time.Hour))
	remainder -= time.Duration(days) * 24 * time.Hour
	hours := int(remainder / time.Hour)
	remainder -= time.Duration(hours) * time.Hour
	minutes := int(remainder / time.Minute)

	parts := make([]string, 0, 5)
	for _, part := range []struct {
		value  int
		suffix string
	}{
		{years, "y"},
		{months, "M"},
		{days, "d"},
		{hours, "h"},
		{minutes, "m"},
	} {
		if part.value > 0 {
			parts = append(parts, fmt.Sprintf("%d%s", part.value, part.suffix))
		}
	}
	if len(parts) == 0 {
		if now.Equal(event) {
			return "0m"
		}
		return "<1m"
	}
	return strings.Join(parts, "")
}

func addCalendarMonthsClamped(value time.Time, months int) time.Time {
	monthIndex := value.Year()*12 + int(value.Month()) - 1 + months
	year := monthIndex / 12
	month := time.Month(monthIndex%12 + 1)
	lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, value.Location()).Day()
	day := value.Day()
	if day > lastDay {
		day = lastDay
	}
	return time.Date(year, month, day, value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), value.Location())
}

func PrintMaintenanceWindows(w io.Writer, windows []storage.MaintenanceWindow, now time.Time) error {
	return printTable(w, func(tw *tabwriter.Writer) error {
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
		return nil
	})
}

func PrintIncidents(w io.Writer, incidents []storage.Incident) error {
	return printTable(w, func(tw *tabwriter.Writer) error {
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
		return nil
	})
}

func PrintStatusIntervals(w io.Writer, intervals []storage.StatusInterval) error {
	return PrintStatusIntervalsAt(w, intervals, time.Now())
}

func PrintStatusIntervalsAt(w io.Writer, intervals []storage.StatusInterval, now time.Time) error {
	return printTable(w, func(tw *tabwriter.Writer) error {
		if _, err := fmt.Fprintln(tw, "START\tEND\tDURATION\tDOWNTIME\tSTATUS\tMONITOR"); err != nil {
			return err
		}
		for _, interval := range intervals {
			downtime := "no"
			if interval.Downtime {
				downtime = "yes"
			}
			if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
				formatCLITime(interval.StartedAt),
				formatCLITime(interval.EndedAt),
				formatIntervalDuration(interval, now),
				downtime,
				interval.Status,
				interval.MonitorID,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func formatIntervalDuration(interval storage.StatusInterval, now time.Time) string {
	if interval.StartedAt.IsZero() {
		return "-"
	}
	end := interval.EndedAt
	if end.IsZero() {
		end = now
	}
	if end.Before(interval.StartedAt) {
		return "-"
	}
	return end.Sub(interval.StartedAt).Truncate(time.Second).String()
}

func PrintFailures(w io.Writer, failedProbes []storage.ProbeResult, observerState storage.ObserverState, observerKnown bool, sentinelEvents []storage.ObserverSentinelResult) error {
	if len(failedProbes) > 0 {
		if err := printTable(w, func(tw *tabwriter.Writer) error {
			if _, err := fmt.Fprintln(tw, "TIME\tMONITOR\tSTATUS\tFAILURE TYPE\tSUPPRESSION\tERROR"); err != nil {
				return err
			}
			for _, p := range failedProbes {
				suppression := "-"
				if p.ObserverSuppressed {
					suppression = "observer down"
				} else if p.ConnectivitySuppressed {
					suppression = "slow Cloudflare transfer"
				}
				if _, err := fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\n", formatCLITime(p.CheckedAt), p.MonitorID, p.ObservedStatusCode, formatFailureKind(p.FailureKind), suppression, p.Error); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	if len(sentinelEvents) > 0 {
		if len(failedProbes) > 0 {
			fmt.Fprintln(w)
		}
		if err := printTable(w, func(tw *tabwriter.Writer) error {
			if _, err := fmt.Fprintln(tw, "TIME\tSENTINEL\tSTATUS\tLATENCY\tERROR"); err != nil {
				return err
			}
			for _, se := range sentinelEvents {
				status := "FAIL"
				if se.OK {
					status = "OK"
				}
				if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%dms\t%s\n", formatCLITime(se.CheckedAt), se.SentinelID, status, se.LatencyMS, se.Error); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	if observerKnown &&
		(len(failedProbes) > 0 || len(sentinelEvents) > 0) &&
		(observerState.Status == "OBSERVER_DOWN" || observerState.ConsecutiveFailures > 0) {
		fmt.Fprintf(w, "\nOBSERVER: %s (%d failures)\n", observerState.Status, observerState.ConsecutiveFailures)
	}

	return nil
}

func formatFailureKind(kind string) string {
	switch kind {
	case "timeout":
		return "request timed out"
	case "transport":
		return "connection error"
	case "status_code":
		return "wrong HTTP status"
	case "body_read":
		return "response read error"
	case "body_assertion":
		return "response body check"
	case "redirect_target":
		return "redirect target check"
	case "max_response_time":
		return "response too slow"
	case "invalid_request":
		return "invalid request"
	default:
		return "-"
	}
}

func PrintObserverStats(w io.Writer, response controlapi.ObserverStatsResponse) error {
	if len(response.Series) == 0 {
		fmt.Fprintf(w, "No connectivity samples since %s.\n", formatCLITime(response.Since))
		if response.TransferCheckEnabled {
			fmt.Fprintf(w, "Cloudflare transfer slow limit: %dms; confirmation: %d consecutive slow or failed samples.\n", response.CurrentSlowLimitMS, response.ConsecutiveSlowResults)
		}
		return nil
	}
	fmt.Fprintf(w, "Connectivity samples since %s (generated %s)\n", formatCLITime(response.Since), formatCLITime(response.GeneratedAt))
	if response.TransferCheckEnabled {
		fmt.Fprintf(w, "Cloudflare transfer: timeout %dms, slow limit %dms, confirmation %d consecutive slow or failed samples.\n", response.TransferTimeoutMS, response.CurrentSlowLimitMS, response.ConsecutiveSlowResults)
	}
	if err := printTable(w, func(tw *tabwriter.Writer) error {
		if _, err := fmt.Fprintln(tw, "KIND\tTARGET\tSAMPLES\tOK\tFAILED\tSLOW\tAVG\tP50\tP95\tMAX\tSOURCES"); err != nil {
			return err
		}
		for _, series := range response.Series {
			target := series.TargetName
			if target == "" {
				target = series.TargetID
			}
			sources := make([]string, 0, len(series.SourceCounts))
			for source, count := range series.SourceCounts {
				sources = append(sources, fmt.Sprintf("%s:%d", source, count))
			}
			sort.Strings(sources)
			if _, err := fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%d\t%dms\t%dms\t%dms\t%dms\t%s\n",
				series.Kind, target, series.Samples, series.SuccessfulSamples, series.FailedSamples,
				series.SlowSamples, series.AverageMS, series.P50MS, series.P95MS, series.MaxMS, strings.Join(sources, ",")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for _, series := range response.Series {
		if series.RecommendedSlowLimitMS > 0 {
			fmt.Fprintf(w, "\nCloudflare slow limit suggestion: %dms. %s\n", series.RecommendedSlowLimitMS, series.Recommendation)
		} else if series.Recommendation != "" {
			fmt.Fprintf(w, "\nCloudflare slow limit: %s\n", series.Recommendation)
		}
	}
	return nil
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
