// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package getmetricdata

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/model"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/promutil"
)

// maxSteadyStateDelay is the maximum delay (in seconds) applied to steady-state queries.
// CloudWatch publishing delay is ~2-10 minutes regardless of metric period, so there's
// no need to shift the window further back for large-period metrics (5m, 1h, 24h).
const maxSteadyStateDelay int64 = 600

// steadyStateMultiplier defines how many periods of timeSince are considered "steady state".
// With CW publishing delay (~2min) and our delay offset, cached_last is typically 2-3 periods
// behind the current scrape time. The multiplier adds buffer for timing variance.
const steadyStateMultiplier int64 = 5

// CachingProcessorConfig holds configuration for the CachingProcessor.
type CachingProcessorConfig struct {
	// MinPeriods is the number of periods to look back on cache miss (cold start).
	// On the first scrape for a timeseries there is no cached state, so we query
	// this many periods. Keeping it at 1 means "just fetch 1 period; if CW has
	// nothing yet the next scrape will try again".
	// Default: 1
	MinPeriods int64

	// MaxPeriods is the maximum number of periods the lookback window can grow to
	// on cache hit. When a gap is detected the window expands from 1 period up to
	// MaxPeriods * period, then stays capped there. This prevents excessively
	// large queries after long outages while still recovering short gaps.
	// Default: 5
	MaxPeriods int64
}

// DefaultCachingProcessorConfig returns a CachingProcessorConfig with sensible defaults.
func DefaultCachingProcessorConfig() CachingProcessorConfig {
	return CachingProcessorConfig{
		MinPeriods: 1,
		MaxPeriods: 5,
	}
}

// requestMetadata holds pre-computed information about a request that we need
// after the inner processor clears GetMetricDataProcessingParams.
type requestMetadata struct {
	cacheKey  uint64
	period    int64
	statistic string
}

// CachingProcessor wraps an inner processor (typically getmetricdata.Processor) and adds
// a timeseries cache layer. It adjusts the lookback window to at least minPeriods * period,
// detects gaps via the cache to extend the window further, and deduplicates data points
// that have already been seen in previous scrapes.
type CachingProcessor struct {
	inner  Processor
	cache  *TimeseriesCache
	config CachingProcessorConfig
	logger *slog.Logger
}

// NewCachingProcessor creates a CachingProcessor that wraps the given inner processor
// with cache-based window adjustment and deduplication.
func NewCachingProcessor(logger *slog.Logger, inner Processor, cache *TimeseriesCache, config CachingProcessorConfig) *CachingProcessor {
	return &CachingProcessor{
		inner:  inner,
		cache:  cache,
		config: config,
		logger: logger,
	}
}

// Run implements the same interface as Processor.Run. It adjusts the lookback window
// on each request based on cached state, delegates to the inner processor, then
// deduplicates results and updates the cache.
func (cp *CachingProcessor) Run(ctx context.Context, namespace string, requests []*model.CloudwatchData) ([]*model.CloudwatchData, error) {
	if len(requests) == 0 {
		return requests, nil
	}

	metadata := cp.adjustRequestWindows(namespace, requests)

	// Delegate to inner processor. Steady-state metrics have delay=period and gapped
	// metrics have delay=0, so the Iterator naturally creates separate batches with
	// different time windows — no manual splitting needed.
	results, err := cp.inner.Run(ctx, namespace, requests)
	if err != nil {
		return nil, err
	}

	cp.deduplicateAndUpdateCache(namespace, results, metadata)

	return results, nil
}

// cappedDelay returns the delay for a given period, capped at maxSteadyStateDelay.
func cappedDelay(period int64) int64 {
	if period > maxSteadyStateDelay {
		return maxSteadyStateDelay
	}
	return period
}

// adjustRequestWindows sets the Length and Delay on each request based on cached state.
// Returns metadata for post-processing.
func (cp *CachingProcessor) adjustRequestWindows(namespace string, requests []*model.CloudwatchData) map[*model.CloudwatchData]requestMetadata {
	metadata := make(map[*model.CloudwatchData]requestMetadata, len(requests))
	now := time.Now()

	for _, req := range requests {
		if req.GetMetricDataProcessingParams == nil {
			continue
		}

		period := req.GetMetricDataProcessingParams.Period
		statistic := req.GetMetricDataProcessingParams.Statistic
		key := BuildCacheKey(namespace, req.MetricName, req.Dimensions, statistic)

		metadata[req] = requestMetadata{
			cacheKey:  key,
			period:    period,
			statistic: statistic,
		}

		cached, ok := cp.cache.Get(key)
		if !ok {
			promutil.TimeseriesCacheMissCounter.Inc()
			cp.applyColdStartWindow(req, period)
			continue
		}

		promutil.TimeseriesCacheHitCounter.Inc()

		timeSinceSeconds := int64(now.Sub(cached.LastTimestamp).Seconds())
		if timeSinceSeconds <= steadyStateMultiplier*period {
			cp.applySteadyStateWindow(req, period)
		} else {
			cp.applyGapRecoveryWindow(req, namespace, period, timeSinceSeconds, period*cp.config.MaxPeriods)
		}
	}

	return metadata
}

// applyColdStartWindow sets the window for a metric with no cached state.
// Uses MinPeriods lookback with a capped delay to fetch guaranteed-published data.
func (cp *CachingProcessor) applyColdStartWindow(req *model.CloudwatchData, period int64) {
	req.GetMetricDataProcessingParams.Length = period * cp.config.MinPeriods
	req.GetMetricDataProcessingParams.Delay = cappedDelay(period)
}

// applySteadyStateWindow sets a tight 1-period window with delay for normal operation.
// The delay shifts the window into the past where CW has definitely published,
// eliminating empty fetches and producing adjacent non-overlapping windows
// between consecutive scrapes (zero dedup waste).
func (cp *CachingProcessor) applySteadyStateWindow(req *model.CloudwatchData, period int64) {
	req.GetMetricDataProcessingParams.Length = period
	req.GetMetricDataProcessingParams.Delay = cappedDelay(period)
}

// applyGapRecoveryWindow sets an extended window to recover missed data after a gap.
// Uses delay=0 to reach as close to "now" as possible, with length capped at maxLength.
func (cp *CachingProcessor) applyGapRecoveryWindow(req *model.CloudwatchData, namespace string, period, timeSinceSeconds, maxLength int64) {
	neededLength := timeSinceSeconds + period
	if neededLength > maxLength {
		promutil.TimeseriesCacheGapDetectedCounter.Inc()
		cp.logger.Warn("[GAP_CAPPED] lookback capped at max",
			"namespace", namespace,
			"metric", req.MetricName,
			"key", BuildCacheKey(namespace, req.MetricName, req.Dimensions, req.GetMetricDataProcessingParams.Statistic),
			"needed_seconds", neededLength,
			"capped_to", maxLength,
		)
		neededLength = maxLength
	}
	req.GetMetricDataProcessingParams.Length = neededLength
	req.GetMetricDataProcessingParams.Delay = 0
}

// deduplicateAndUpdateCache filters out already-seen datapoints and advances the cache.
func (cp *CachingProcessor) deduplicateAndUpdateCache(namespace string, results []*model.CloudwatchData, metadata map[*model.CloudwatchData]requestMetadata) {
	for _, result := range results {
		if result.GetMetricDataResult == nil {
			continue
		}

		meta, ok := metadata[result]
		if !ok {
			continue
		}

		totalRaw := len(result.GetMetricDataResult.DataPoints)
		newCount := cp.deduplicateDataPoints(result, meta)

		if newCount == 0 && totalRaw == 0 {
			cp.detectActualGap(namespace, result, meta)
		}

		cp.updateCacheFromResult(result, meta)
	}
}

// deduplicateDataPoints filters out datapoints at or before the cached timestamp.
// Returns the count of new (non-duplicate) points.
func (cp *CachingProcessor) deduplicateDataPoints(result *model.CloudwatchData, meta requestMetadata) int {
	cached, hasCached := cp.cache.Get(meta.cacheKey)
	if !hasCached {
		return len(result.GetMetricDataResult.DataPoints)
	}

	totalRaw := len(result.GetMetricDataResult.DataPoints)
	filtered := make([]model.DataPoint, 0, totalRaw)
	for _, dp := range result.GetMetricDataResult.DataPoints {
		if dp.Timestamp.After(cached.LastTimestamp) {
			filtered = append(filtered, dp)
		}
	}

	if dedupCount := totalRaw - len(filtered); dedupCount > 0 {
		promutil.TimeseriesDedupCounter.Add(float64(dedupCount))
	}

	result.GetMetricDataResult.DataPoints = filtered
	return len(filtered)
}

// detectActualGap checks if a zero-datapoint result represents a genuine CloudWatch gap
// (as opposed to a metric that simply hasn't been published yet).
func (cp *CachingProcessor) detectActualGap(namespace string, result *model.CloudwatchData, meta requestMetadata) {
	cached, hasCached := cp.cache.Get(meta.cacheKey)
	if !hasCached {
		return
	}

	timeSinceLast := time.Since(cached.LastTimestamp)
	if timeSinceLast <= time.Duration(meta.period)*time.Second {
		return
	}

	promutil.TimeseriesEmptyScrapeCounter.WithLabelValues(namespace, result.MetricName, meta.statistic).Inc()
}

// updateCacheFromResult advances the cache to the newest datapoint timestamp in the result.
func (cp *CachingProcessor) updateCacheFromResult(result *model.CloudwatchData, meta requestMetadata) {
	var newestTimestamp time.Time
	for _, dp := range result.GetMetricDataResult.DataPoints {
		if dp.Timestamp.After(newestTimestamp) {
			newestTimestamp = dp.Timestamp
		}
	}
	if !newestTimestamp.IsZero() {
		cp.cache.Set(meta.cacheKey, TimeseriesCacheEntry{
			LastTimestamp: newestTimestamp,
			Interval:      meta.period,
		})
	}
}
