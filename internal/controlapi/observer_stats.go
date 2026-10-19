package controlapi

import (
	"math"
	"sort"
	"time"

	"upag/internal/config"
	"upag/internal/storage"
)

type ConnectivitySeries struct {
	Kind                   string         `json:"kind"`
	TargetID               string         `json:"target_id"`
	TargetName             string         `json:"target_name"`
	URL                    string         `json:"url"`
	Samples                int            `json:"samples"`
	SuccessfulSamples      int            `json:"successful_samples"`
	FailedSamples          int            `json:"failed_samples"`
	SlowSamples            int            `json:"slow_samples"`
	AverageMS              int64          `json:"average_ms"`
	P50MS                  int64          `json:"p50_ms"`
	P95MS                  int64          `json:"p95_ms"`
	MaxMS                  int64          `json:"max_ms"`
	SourceCounts           map[string]int `json:"source_counts"`
	TriggerMonitorIDs      []string       `json:"trigger_monitor_ids,omitempty"`
	FirstSampleAt          time.Time      `json:"first_sample_at"`
	LastSampleAt           time.Time      `json:"last_sample_at"`
	RecommendedSlowLimitMS int64          `json:"recommended_slow_limit_ms,omitempty"`
	Recommendation         string         `json:"recommendation,omitempty"`
}

type ObserverStatsResponse struct {
	GeneratedAt            time.Time            `json:"generated_at"`
	Since                  time.Time            `json:"since"`
	TransferCheckEnabled   bool                 `json:"transfer_check_enabled"`
	TransferTimeoutMS      int64                `json:"transfer_timeout_ms"`
	CurrentSlowLimitMS     int64                `json:"current_slow_limit_ms"`
	ConsecutiveSlowResults int                  `json:"consecutive_slow_results"`
	Series                 []ConnectivitySeries `json:"series"`
}

func ObserverStatsQuerySince(since, generatedAt time.Time, transfer config.TransferCheckConfig) time.Time {
	sevenDayCutoff := generatedAt.UTC().Add(-7 * 24 * time.Hour)
	coverageInterval := transfer.Interval.Duration
	if coverageInterval <= 0 {
		coverageInterval = 15 * time.Minute
	}
	coverageSince := sevenDayCutoff.Add(-coverageInterval)
	if since.IsZero() || since.After(coverageSince) {
		return coverageSince
	}
	return since.UTC()
}

func ObserverStatsFromStorage(samples []storage.ConnectivitySample, since, generatedAt time.Time, transfer config.TransferCheckConfig) ObserverStatsResponse {
	type accumulator struct {
		series                   ConnectivitySeries
		elapsed                  []int64
		recommendationSuccessful []int64
		elapsedTotal             int64
		monitors                 map[string]bool
	}
	groups := map[string]*accumulator{}
	coverageFirstAt := map[string]time.Time{}
	sevenDayCutoff := generatedAt.UTC().Add(-7 * 24 * time.Hour)
	for _, sample := range samples {
		key := sample.Kind + "\x00" + sample.TargetID
		if sample.Kind == "transfer" && !sample.CheckedAt.After(sevenDayCutoff) {
			first := coverageFirstAt[key]
			if first.IsZero() || sample.CheckedAt.Before(first) {
				coverageFirstAt[key] = sample.CheckedAt
			}
		}
		if sample.CheckedAt.Before(since) {
			continue
		}
		group := groups[key]
		if group == nil {
			group = &accumulator{series: ConnectivitySeries{
				Kind: sample.Kind, TargetID: sample.TargetID, TargetName: sample.TargetName,
				URL: sample.URL, SourceCounts: map[string]int{}, FirstSampleAt: sample.CheckedAt, LastSampleAt: sample.CheckedAt,
			}, monitors: map[string]bool{}}
			groups[key] = group
		}
		group.series.Samples++
		group.series.SourceCounts[sample.Source]++
		if sample.MonitorID != "" {
			group.monitors[sample.MonitorID] = true
		}
		elapsed := sample.LatencyMS
		if sample.Kind == "transfer" {
			elapsed = sample.ResponseTimeMS
		}
		if elapsed < 0 {
			elapsed = 0
		}
		group.elapsed = append(group.elapsed, elapsed)
		group.elapsedTotal += elapsed
		if sample.OK {
			group.series.SuccessfulSamples++
			if sample.Kind == "transfer" && !sample.CheckedAt.Before(sevenDayCutoff) {
				group.recommendationSuccessful = append(group.recommendationSuccessful, elapsed)
			}
		} else {
			group.series.FailedSamples++
		}
		if sample.Slow {
			group.series.SlowSamples++
		}
		if sample.CheckedAt.After(group.series.LastSampleAt) {
			group.series.LastSampleAt = sample.CheckedAt
		}
		if sample.CheckedAt.Before(group.series.FirstSampleAt) {
			group.series.FirstSampleAt = sample.CheckedAt
		}
	}

	response := ObserverStatsResponse{
		GeneratedAt: generatedAt.UTC(), Since: since.UTC(),
		TransferCheckEnabled:   transfer.Enabled,
		TransferTimeoutMS:      transfer.Timeout.Duration.Milliseconds(),
		CurrentSlowLimitMS:     transfer.SlowThreshold.Duration.Milliseconds(),
		ConsecutiveSlowResults: transfer.ConsecutiveSlowResults,
		Series:                 make([]ConnectivitySeries, 0, len(groups)),
	}
	for _, group := range groups {
		series := group.series
		if len(group.elapsed) > 0 {
			sort.Slice(group.elapsed, func(i, j int) bool { return group.elapsed[i] < group.elapsed[j] })
			series.AverageMS = group.elapsedTotal / int64(len(group.elapsed))
			series.P50MS = percentile(group.elapsed, 0.50)
			series.P95MS = percentile(group.elapsed, 0.95)
			series.MaxMS = group.elapsed[len(group.elapsed)-1]
		}
		for monitorID := range group.monitors {
			series.TriggerMonitorIDs = append(series.TriggerMonitorIDs, monitorID)
		}
		sort.Strings(series.TriggerMonitorIDs)
		if series.Kind == "transfer" {
			key := series.Kind + "\x00" + series.TargetID
			if since.After(sevenDayCutoff) || coverageFirstAt[key].IsZero() {
				series.Recommendation = "Collect a full seven days of samples before adjusting the slow limit."
			} else if len(group.recommendationSuccessful) < 20 {
				series.Recommendation = "Collect at least 20 successful transfer samples before adjusting the slow limit."
			} else {
				sort.Slice(group.recommendationSuccessful, func(i, j int) bool { return group.recommendationSuccessful[i] < group.recommendationSuccessful[j] })
				p95 := percentile(group.recommendationSuccessful, 0.95)
				recommended := int64(math.Ceil(float64(p95)*1.5/500.0) * 500)
				series.RecommendedSlowLimitMS = recommended
				series.Recommendation = "Advisory only: 1.5 times the successful transfer P95, rounded up to 500 ms."
			}
		}
		response.Series = append(response.Series, series)
	}
	sort.Slice(response.Series, func(i, j int) bool {
		if response.Series[i].Kind == response.Series[j].Kind {
			return response.Series[i].TargetID < response.Series[j].TargetID
		}
		return response.Series[i].Kind < response.Series[j].Kind
	})
	return response
}

func percentile(sorted []int64, fraction float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(len(sorted))*fraction)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}
