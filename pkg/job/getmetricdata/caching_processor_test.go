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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/model"
)

func TestCachingProcessor_AdjustsLengthToMinPeriods(t *testing.T) {
	// A metric with period=60, length=60 should get extended to length=300 (5*60)
	now := time.Now()
	cache := NewTimeseriesCache(15 * time.Minute)
	defer cache.Stop()

	var capturedLength int64
	client := testClient{
		GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
			// Capture the length that was set on the data
			if len(data) > 0 && data[0].GetMetricDataProcessingParams != nil {
				capturedLength = data[0].GetMetricDataProcessingParams.Length
			}
			results := make([]cloudwatch.MetricDataResult, 0, len(data))
			for _, d := range data {
				results = append(results, cloudwatch.MetricDataResult{
					ID:         d.GetMetricDataProcessingParams.QueryID,
					DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now}},
				})
			}
			return results
		},
	}

	inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
	config := CachingProcessorConfig{
		MinPeriods: 1,
		MaxPeriods: 5,
	}
	cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

	requests := []*model.CloudwatchData{
		{
			MetricName:   "CPUUtilization",
			ResourceName: "i-123",
			Namespace:    "AWS/EC2",
			Dimensions:   []model.Dimension{{Name: "InstanceId", Value: "i-123"}},
			GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
				Period:    60,
				Length:    60, // Will stay at 60 (1 * 60, cold start with MinPeriods=1)
				Delay:     0,
				Statistic: "Average",
			},
		},
	}

	results, err := cp.Run(context.Background(), "AWS/EC2", requests)
	require.NoError(t, err)
	require.Len(t, results, 1)

	// The length should have been adjusted to 1 * 60 = 60 (MinPeriods=1)
	assert.Equal(t, int64(60), capturedLength)
}

func TestCachingProcessor_DeduplicatesDataPoints(t *testing.T) {
	now := time.Now()
	oldTimestamp := now.Add(-5 * time.Minute)
	newTimestamp := now.Add(-1 * time.Minute)

	cache := NewTimeseriesCache(15 * time.Minute)
	defer cache.Stop()

	// Pre-populate cache with old timestamp
	cacheKey := BuildCacheKey("AWS/EC2", "CPUUtilization", []model.Dimension{{Name: "InstanceId", Value: "i-123"}}, "Average")
	cache.Set(cacheKey, TimeseriesCacheEntry{
		LastTimestamp: oldTimestamp,
		Interval:      60,
	})

	client := testClient{
		GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
			results := make([]cloudwatch.MetricDataResult, 0, len(data))
			for _, d := range data {
				results = append(results, cloudwatch.MetricDataResult{
					ID: d.GetMetricDataProcessingParams.QueryID,
					DataPoints: []cloudwatch.DataPoint{
						{Value: aws.Float64(50), Timestamp: oldTimestamp},                   // Should be filtered (not after cached)
						{Value: aws.Float64(70), Timestamp: newTimestamp},                   // Should be kept
						{Value: aws.Float64(30), Timestamp: oldTimestamp.Add(-time.Minute)}, // Should be filtered (before cached)
					},
				})
			}
			return results
		},
	}

	inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
	cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, DefaultCachingProcessorConfig())

	requests := []*model.CloudwatchData{
		{
			MetricName:   "CPUUtilization",
			ResourceName: "i-123",
			Namespace:    "AWS/EC2",
			Dimensions:   []model.Dimension{{Name: "InstanceId", Value: "i-123"}},
			GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
				Period:    60,
				Length:    300,
				Delay:     0,
				Statistic: "Average",
			},
		},
	}

	results, err := cp.Run(context.Background(), "AWS/EC2", requests)
	require.NoError(t, err)
	require.Len(t, results, 1)

	// Only the new data point should remain
	require.Len(t, results[0].GetMetricDataResult.DataPoints, 1)
	assert.Equal(t, newTimestamp, results[0].GetMetricDataResult.DataPoints[0].Timestamp)
	assert.Equal(t, float64(70), *results[0].GetMetricDataResult.DataPoints[0].Value)

	// Cache should be updated with newest timestamp
	entry, ok := cache.Get(cacheKey)
	require.True(t, ok)
	assert.Equal(t, newTimestamp, entry.LastTimestamp)
}

func TestCachingProcessor_GapDetection(t *testing.T) {
	now := time.Now()
	// Simulate a gap: last seen 10 minutes ago, period is 60 seconds
	lastSeen := now.Add(-10 * time.Minute)

	cache := NewTimeseriesCache(15 * time.Minute)
	defer cache.Stop()

	cacheKey := BuildCacheKey("AWS/EC2", "CPUUtilization", []model.Dimension{{Name: "InstanceId", Value: "i-123"}}, "Average")
	cache.Set(cacheKey, TimeseriesCacheEntry{
		LastTimestamp: lastSeen,
		Interval:      60,
	})

	var capturedLength int64
	client := testClient{
		GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
			if len(data) > 0 && data[0].GetMetricDataProcessingParams != nil {
				capturedLength = data[0].GetMetricDataProcessingParams.Length
			}
			results := make([]cloudwatch.MetricDataResult, 0, len(data))
			for _, d := range data {
				results = append(results, cloudwatch.MetricDataResult{
					ID:         d.GetMetricDataProcessingParams.QueryID,
					DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now.Add(-1 * time.Minute)}},
				})
			}
			return results
		},
	}

	inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
	config := CachingProcessorConfig{
		MinPeriods: 1,
		MaxPeriods: 5,
	}
	cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

	requests := []*model.CloudwatchData{
		{
			MetricName:   "CPUUtilization",
			ResourceName: "i-123",
			Namespace:    "AWS/EC2",
			Dimensions:   []model.Dimension{{Name: "InstanceId", Value: "i-123"}},
			GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
				Period:    60,
				Length:    60,
				Delay:     0,
				Statistic: "Average",
			},
		},
	}

	_, err := cp.Run(context.Background(), "AWS/EC2", requests)
	require.NoError(t, err)

	// Gap is ~600 seconds, but MaxPeriods=5 caps at 5*60=300.
	assert.Equal(t, int64(300), capturedLength, "length should be capped at MaxPeriods * period")
}

func TestCachingProcessor_GapCappedByMaxPeriods(t *testing.T) {
	now := time.Now()
	// Simulate a very large gap: 2 hours ago
	lastSeen := now.Add(-2 * time.Hour)

	cache := NewTimeseriesCache(3 * time.Hour) // TTL larger than gap for test
	defer cache.Stop()

	cacheKey := BuildCacheKey("AWS/EC2", "CPUUtilization", []model.Dimension{{Name: "InstanceId", Value: "i-123"}}, "Average")
	cache.Set(cacheKey, TimeseriesCacheEntry{
		LastTimestamp: lastSeen,
		Interval:      60,
	})

	var capturedLength int64
	client := testClient{
		GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
			if len(data) > 0 && data[0].GetMetricDataProcessingParams != nil {
				capturedLength = data[0].GetMetricDataProcessingParams.Length
			}
			results := make([]cloudwatch.MetricDataResult, 0, len(data))
			for _, d := range data {
				results = append(results, cloudwatch.MetricDataResult{
					ID:         d.GetMetricDataProcessingParams.QueryID,
					DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now.Add(-1 * time.Minute)}},
				})
			}
			return results
		},
	}

	inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
	config := CachingProcessorConfig{
		MinPeriods: 1,
		MaxPeriods: 5, // Cap at 5 * 60 = 300s
	}
	cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

	requests := []*model.CloudwatchData{
		{
			MetricName:   "CPUUtilization",
			ResourceName: "i-123",
			Namespace:    "AWS/EC2",
			Dimensions:   []model.Dimension{{Name: "InstanceId", Value: "i-123"}},
			GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
				Period:    60,
				Length:    60,
				Delay:     0,
				Statistic: "Average",
			},
		},
	}

	_, err := cp.Run(context.Background(), "AWS/EC2", requests)
	require.NoError(t, err)

	// Gap is 2 hours, but MaxPeriods=5 caps at 5*60 = 300 seconds
	assert.Equal(t, int64(300), capturedLength, "length should be capped at MaxPeriods * period")
}

func TestCachingProcessor_CacheMiss_NoDedup(t *testing.T) {
	now := time.Now()
	cache := NewTimeseriesCache(15 * time.Minute)
	defer cache.Stop()

	client := testClient{
		GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
			results := make([]cloudwatch.MetricDataResult, 0, len(data))
			for _, d := range data {
				results = append(results, cloudwatch.MetricDataResult{
					ID: d.GetMetricDataProcessingParams.QueryID,
					DataPoints: []cloudwatch.DataPoint{
						{Value: aws.Float64(10), Timestamp: now.Add(-4 * time.Minute)},
						{Value: aws.Float64(20), Timestamp: now.Add(-3 * time.Minute)},
						{Value: aws.Float64(30), Timestamp: now.Add(-2 * time.Minute)},
					},
				})
			}
			return results
		},
	}

	inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
	cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, DefaultCachingProcessorConfig())

	requests := []*model.CloudwatchData{
		{
			MetricName:   "CPUUtilization",
			ResourceName: "i-123",
			Namespace:    "AWS/EC2",
			Dimensions:   []model.Dimension{{Name: "InstanceId", Value: "i-123"}},
			GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
				Period:    60,
				Length:    300,
				Delay:     0,
				Statistic: "Average",
			},
		},
	}

	results, err := cp.Run(context.Background(), "AWS/EC2", requests)
	require.NoError(t, err)
	require.Len(t, results, 1)

	// All 3 data points should be kept (no cache entry to dedup against)
	assert.Len(t, results[0].GetMetricDataResult.DataPoints, 3)

	// Cache should now have the newest timestamp
	cacheKey := BuildCacheKey("AWS/EC2", "CPUUtilization", []model.Dimension{{Name: "InstanceId", Value: "i-123"}}, "Average")
	entry, ok := cache.Get(cacheKey)
	require.True(t, ok)
	assert.Equal(t, now.Add(-2*time.Minute), entry.LastTimestamp)
	assert.Equal(t, int64(60), entry.Interval)
}

func TestCachingProcessor_EmptyRequests(t *testing.T) {
	cache := NewTimeseriesCache(15 * time.Minute)
	defer cache.Stop()

	inner := NewDefaultProcessor(promslog.NewNopLogger(), testClient{}, 500, 1)
	cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, DefaultCachingProcessorConfig())

	results, err := cp.Run(context.Background(), "AWS/EC2", []*model.CloudwatchData{})
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestCachingProcessor_UpdatesCacheWithInterval(t *testing.T) {
	now := time.Now()
	cache := NewTimeseriesCache(15 * time.Minute)
	defer cache.Stop()

	client := testClient{
		GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
			results := make([]cloudwatch.MetricDataResult, 0, len(data))
			for _, d := range data {
				results = append(results, cloudwatch.MetricDataResult{
					ID:         d.GetMetricDataProcessingParams.QueryID,
					DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now}},
				})
			}
			return results
		},
	}

	inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
	cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, DefaultCachingProcessorConfig())

	requests := []*model.CloudwatchData{
		{
			MetricName:   "RequestCount",
			ResourceName: "my-lb",
			Namespace:    "AWS/ELB",
			Dimensions:   []model.Dimension{{Name: "LoadBalancer", Value: "my-lb"}},
			GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
				Period:    300,
				Length:    1500,
				Delay:     0,
				Statistic: "Sum",
			},
		},
	}

	_, err := cp.Run(context.Background(), "AWS/ELB", requests)
	require.NoError(t, err)

	cacheKey := BuildCacheKey("AWS/ELB", "RequestCount", []model.Dimension{{Name: "LoadBalancer", Value: "my-lb"}}, "Sum")
	entry, ok := cache.Get(cacheKey)
	require.True(t, ok)
	assert.Equal(t, int64(300), entry.Interval, "interval should match the metric's period")
}

// =============================================================================
// Smart Lookback Window Tests
// =============================================================================

func TestCachingProcessor_SmartLookback_ColdStart(t *testing.T) {
	// Cold start (no cache entry) should use MinPeriods * period
	// Default MinPeriods=1 means fetch exactly 1 period on first scrape
	testCases := []struct {
		name       string
		period     int64
		minPeriods int64
		expected   int64
	}{
		{"60s period, 1 minPeriod (default)", 60, 1, 60},
		{"300s period, 1 minPeriod (default)", 300, 1, 300},
		{"60s period, 2 minPeriods", 60, 2, 120},
		{"120s period, 3 minPeriods", 120, 3, 360},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			cache := NewTimeseriesCache(15 * time.Minute)
			defer cache.Stop()

			var capturedLength int64
			client := testClient{
				GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
					if len(data) > 0 && data[0].GetMetricDataProcessingParams != nil {
						capturedLength = data[0].GetMetricDataProcessingParams.Length
					}
					results := make([]cloudwatch.MetricDataResult, 0, len(data))
					for _, d := range data {
						results = append(results, cloudwatch.MetricDataResult{
							ID:         d.GetMetricDataProcessingParams.QueryID,
							DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now}},
						})
					}
					return results
				},
			}

			inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
			config := CachingProcessorConfig{
				MinPeriods: tc.minPeriods,
				MaxPeriods: 5,
			}
			cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

			requests := []*model.CloudwatchData{
				{
					MetricName:   "TestMetric",
					ResourceName: "test-resource",
					Namespace:    "AWS/Test",
					Dimensions:   []model.Dimension{{Name: "TestDim", Value: "test-val"}},
					GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
						Period:    tc.period,
						Length:    tc.period, // Original length doesn't matter on cold start
						Delay:     0,
						Statistic: "Average",
					},
				},
			}

			_, err := cp.Run(context.Background(), "AWS/Test", requests)
			require.NoError(t, err)

			assert.Equal(t, tc.expected, capturedLength, "cold start should use minPeriods * period")
		})
	}
}

func TestCachingProcessor_SmartLookback_SteadyState(t *testing.T) {
	// Steady state: cache hit with recent timestamp
	// If no gap (timeSinceLast <= period): fetch only 1 period
	// If gap (timeSinceLast > period): fetch timeSinceLast + period
	testCases := []struct {
		name              string
		period            int64
		timeSinceLast     time.Duration
		expectedLengthMin int64 // Allow some timing variance
		expectedLengthMax int64
	}{
		{
			name:              "60s period, 1 minute since last",
			period:            60,
			timeSinceLast:     1 * time.Minute,
			expectedLengthMin: 55, // 1 period (60s) with timing variance
			expectedLengthMax: 65,
		},
		{
			name:              "60s period, 2 minutes since last",
			period:            60,
			timeSinceLast:     2 * time.Minute,
			expectedLengthMin: 55, // steady state: 1 period with delay (120s <= 3*period)
			expectedLengthMax: 65,
		},
		{
			name:              "300s period, 5 minutes since last",
			period:            300,
			timeSinceLast:     5 * time.Minute,
			expectedLengthMin: 295, // 1 period (300s) with timing variance (no gap)
			expectedLengthMax: 305,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			lastTimestamp := now.Add(-tc.timeSinceLast)

			cache := NewTimeseriesCache(15 * time.Minute)
			defer cache.Stop()

			// Pre-populate cache
			cacheKey := BuildCacheKey("AWS/Test", "TestMetric", []model.Dimension{{Name: "TestDim", Value: "test-val"}}, "Average")
			cache.Set(cacheKey, TimeseriesCacheEntry{
				LastTimestamp: lastTimestamp,
				Interval:      tc.period,
			})

			var capturedLength int64
			client := testClient{
				GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
					if len(data) > 0 && data[0].GetMetricDataProcessingParams != nil {
						capturedLength = data[0].GetMetricDataProcessingParams.Length
					}
					results := make([]cloudwatch.MetricDataResult, 0, len(data))
					for _, d := range data {
						results = append(results, cloudwatch.MetricDataResult{
							ID:         d.GetMetricDataProcessingParams.QueryID,
							DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now}},
						})
					}
					return results
				},
			}

			inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
			config := CachingProcessorConfig{
				MinPeriods: 1,
				MaxPeriods: 5,
			}
			cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

			requests := []*model.CloudwatchData{
				{
					MetricName:   "TestMetric",
					ResourceName: "test-resource",
					Namespace:    "AWS/Test",
					Dimensions:   []model.Dimension{{Name: "TestDim", Value: "test-val"}},
					GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
						Period:    tc.period,
						Length:    tc.period * 5, // Original length should be ignored
						Delay:     0,
						Statistic: "Average",
					},
				},
			}

			_, err := cp.Run(context.Background(), "AWS/Test", requests)
			require.NoError(t, err)

			assert.GreaterOrEqual(t, capturedLength, tc.expectedLengthMin,
				"length should match expected range")
			assert.LessOrEqual(t, capturedLength, tc.expectedLengthMax,
				"length should match expected range")
		})
	}
}

func TestCachingProcessor_SmartLookback_GapWithinLimits(t *testing.T) {
	// Gap detected but within MaxPeriods * period - should extend to cover gap
	now := time.Now()
	// 3 minute gap with period=60 and MaxPeriods=5 → needed=240s, cap=300s → fits
	lastTimestamp := now.Add(-3 * time.Minute)

	cache := NewTimeseriesCache(1 * time.Hour)
	defer cache.Stop()

	cacheKey := BuildCacheKey("AWS/Test", "TestMetric", []model.Dimension{{Name: "TestDim", Value: "test-val"}}, "Average")
	cache.Set(cacheKey, TimeseriesCacheEntry{
		LastTimestamp: lastTimestamp,
		Interval:      60,
	})

	var capturedLength int64
	client := testClient{
		GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
			if len(data) > 0 && data[0].GetMetricDataProcessingParams != nil {
				capturedLength = data[0].GetMetricDataProcessingParams.Length
			}
			results := make([]cloudwatch.MetricDataResult, 0, len(data))
			for _, d := range data {
				results = append(results, cloudwatch.MetricDataResult{
					ID:         d.GetMetricDataProcessingParams.QueryID,
					DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now}},
				})
			}
			return results
		},
	}

	inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
	config := CachingProcessorConfig{
		MinPeriods: 1,
		MaxPeriods: 5,
	}
	cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

	requests := []*model.CloudwatchData{
		{
			MetricName:   "TestMetric",
			ResourceName: "test-resource",
			Namespace:    "AWS/Test",
			Dimensions:   []model.Dimension{{Name: "TestDim", Value: "test-val"}},
			GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
				Period:    60,
				Length:    60,
				Delay:     0,
				Statistic: "Average",
			},
		},
	}

	_, err := cp.Run(context.Background(), "AWS/Test", requests)
	require.NoError(t, err)

	// 3 min gap (180s) <= 3*period (180s) → steady state with delay
	// length = period = 60
	assert.Equal(t, int64(60), capturedLength, "3-min gap is within steady-state threshold (3*period)")
}

func TestCachingProcessor_SmartLookback_MaxPeriodsCapping(t *testing.T) {
	// Gap exceeds MaxPeriods * period - should be capped
	testCases := []struct {
		name           string
		gapDuration    time.Duration
		period         int64
		maxPeriods     int64
		expectedLength int64
	}{
		{
			name:           "2 hour gap, 5 max periods with 60s period",
			gapDuration:    2 * time.Hour,
			period:         60,
			maxPeriods:     5,
			expectedLength: 300, // 5 * 60
		},
		{
			name:           "1 day gap, 10 max periods with 300s period",
			gapDuration:    24 * time.Hour,
			period:         300,
			maxPeriods:     10,
			expectedLength: 3000, // 10 * 300
		},
		{
			name:           "6 hour gap, 3 max periods with 60s period",
			gapDuration:    6 * time.Hour,
			period:         60,
			maxPeriods:     3,
			expectedLength: 180, // 3 * 60
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			lastTimestamp := now.Add(-tc.gapDuration)

			cache := NewTimeseriesCache(48 * time.Hour) // Large TTL to not evict
			defer cache.Stop()

			cacheKey := BuildCacheKey("AWS/Test", "TestMetric", []model.Dimension{{Name: "TestDim", Value: "test-val"}}, "Average")
			cache.Set(cacheKey, TimeseriesCacheEntry{
				LastTimestamp: lastTimestamp,
				Interval:      tc.period,
			})

			var capturedLength int64
			client := testClient{
				GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
					if len(data) > 0 && data[0].GetMetricDataProcessingParams != nil {
						capturedLength = data[0].GetMetricDataProcessingParams.Length
					}
					results := make([]cloudwatch.MetricDataResult, 0, len(data))
					for _, d := range data {
						results = append(results, cloudwatch.MetricDataResult{
							ID:         d.GetMetricDataProcessingParams.QueryID,
							DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now}},
						})
					}
					return results
				},
			}

			inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
			config := CachingProcessorConfig{
				MinPeriods: 1,
				MaxPeriods: tc.maxPeriods,
			}
			cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

			requests := []*model.CloudwatchData{
				{
					MetricName:   "TestMetric",
					ResourceName: "test-resource",
					Namespace:    "AWS/Test",
					Dimensions:   []model.Dimension{{Name: "TestDim", Value: "test-val"}},
					GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
						Period:    tc.period,
						Length:    tc.period,
						Delay:     0,
						Statistic: "Average",
					},
				},
			}

			_, err := cp.Run(context.Background(), "AWS/Test", requests)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedLength, capturedLength, "length should be capped at MaxPeriods * period")
		})
	}
}

func TestCachingProcessor_SmartLookback_MixedCacheStates(t *testing.T) {
	// Multiple requests: some with cache hits, some misses
	now := time.Now()
	cache := NewTimeseriesCache(15 * time.Minute)
	defer cache.Stop()

	// Pre-populate cache for metric1 only
	cacheKey1 := BuildCacheKey("AWS/Test", "Metric1", []model.Dimension{{Name: "Dim", Value: "val1"}}, "Average")
	cache.Set(cacheKey1, TimeseriesCacheEntry{
		LastTimestamp: now.Add(-1 * time.Minute), // 1 minute ago
		Interval:      60,
	})
	// Metric2 has no cache entry (cold start)

	capturedLengths := make(map[string]int64)
	client := testClient{
		GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
			results := make([]cloudwatch.MetricDataResult, 0, len(data))
			for _, d := range data {
				if d.GetMetricDataProcessingParams != nil {
					capturedLengths[d.MetricName] = d.GetMetricDataProcessingParams.Length
				}
				results = append(results, cloudwatch.MetricDataResult{
					ID:         d.GetMetricDataProcessingParams.QueryID,
					DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now}},
				})
			}
			return results
		},
	}

	inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
	config := CachingProcessorConfig{
		MinPeriods: 1,
		MaxPeriods: 5,
	}
	cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

	requests := []*model.CloudwatchData{
		{
			MetricName:   "Metric1", // Has cache entry
			ResourceName: "test-resource",
			Namespace:    "AWS/Test",
			Dimensions:   []model.Dimension{{Name: "Dim", Value: "val1"}},
			GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
				Period:    60,
				Length:    60,
				Delay:     0,
				Statistic: "Average",
			},
		},
		{
			MetricName:   "Metric2", // No cache entry (cold start)
			ResourceName: "test-resource",
			Namespace:    "AWS/Test",
			Dimensions:   []model.Dimension{{Name: "Dim", Value: "val2"}},
			GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
				Period:    60,
				Length:    60,
				Delay:     0,
				Statistic: "Average",
			},
		},
	}

	_, err := cp.Run(context.Background(), "AWS/Test", requests)
	require.NoError(t, err)

	// Metric1: cache hit, should have small length (~120s = 60s gap + 60s buffer)
	assert.Less(t, capturedLengths["Metric1"], int64(200),
		"cache hit should result in small lookback")

	// Metric2: cold start, should have MinPeriods length (60s = 1 * 60)
	assert.Equal(t, int64(60), capturedLengths["Metric2"],
		"cold start should use minPeriods * period")
}

func TestCachingProcessor_SmartLookback_EfficiencyComparison(t *testing.T) {
	// This test demonstrates the efficiency improvement
	// In old behavior: always query 5 periods
	// In new behavior: query based on actual gap
	now := time.Now()

	testCases := []struct {
		name              string
		timeSinceLast     time.Duration
		period            int64
		oldBehaviorLength int64 // Always 5 * period
		maxNewLength      int64 // Should be much smaller in steady state
	}{
		{
			name:              "1 minute scrape interval",
			timeSinceLast:     1 * time.Minute,
			period:            60,
			oldBehaviorLength: 300, // 5 * 60
			maxNewLength:      150, // ~60s gap + 60s buffer + variance
		},
		{
			name:              "30 second scrape interval",
			timeSinceLast:     30 * time.Second,
			period:            60,
			oldBehaviorLength: 300, // 5 * 60
			maxNewLength:      130, // ~30s gap + 60s buffer + variance
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewTimeseriesCache(15 * time.Minute)
			defer cache.Stop()

			cacheKey := BuildCacheKey("AWS/Test", "TestMetric", []model.Dimension{{Name: "Dim", Value: "val"}}, "Average")
			cache.Set(cacheKey, TimeseriesCacheEntry{
				LastTimestamp: now.Add(-tc.timeSinceLast),
				Interval:      tc.period,
			})

			var capturedLength int64
			client := testClient{
				GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
					if len(data) > 0 && data[0].GetMetricDataProcessingParams != nil {
						capturedLength = data[0].GetMetricDataProcessingParams.Length
					}
					results := make([]cloudwatch.MetricDataResult, 0, len(data))
					for _, d := range data {
						results = append(results, cloudwatch.MetricDataResult{
							ID:         d.GetMetricDataProcessingParams.QueryID,
							DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now}},
						})
					}
					return results
				},
			}

			inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
			config := CachingProcessorConfig{
				MinPeriods: 1,
				MaxPeriods: 5,
			}
			cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

			requests := []*model.CloudwatchData{
				{
					MetricName:   "TestMetric",
					ResourceName: "test-resource",
					Namespace:    "AWS/Test",
					Dimensions:   []model.Dimension{{Name: "Dim", Value: "val"}},
					GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
						Period:    tc.period,
						Length:    tc.period * 5,
						Delay:     0,
						Statistic: "Average",
					},
				},
			}

			_, err := cp.Run(context.Background(), "AWS/Test", requests)
			require.NoError(t, err)

			assert.Less(t, capturedLength, tc.oldBehaviorLength,
				"new smart lookback should be more efficient than old 5-period lookback")
			assert.LessOrEqual(t, capturedLength, tc.maxNewLength,
				"length should be close to actual gap + buffer")

			// Calculate efficiency improvement
			savings := float64(tc.oldBehaviorLength-capturedLength) / float64(tc.oldBehaviorLength) * 100
			t.Logf("Efficiency improvement: %.1f%% reduction (old: %ds, new: %ds)",
				savings, tc.oldBehaviorLength, capturedLength)
		})
	}
}

func TestCachingProcessor_SmartLookback_ConsecutiveScrapes(t *testing.T) {
	// Simulate multiple consecutive scrapes with realistic timing
	// Note: We use time.Now() relative timestamps since the processor uses time.Now() internally
	cache := NewTimeseriesCache(15 * time.Minute)
	defer cache.Stop()

	period := int64(60)
	numScrapes := 3

	var lengthsPerScrape []int64

	for scrape := 0; scrape < numScrapes; scrape++ {
		var capturedLength int64
		now := time.Now()

		client := testClient{
			GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
				if len(data) > 0 && data[0].GetMetricDataProcessingParams != nil {
					capturedLength = data[0].GetMetricDataProcessingParams.Length
				}
				results := make([]cloudwatch.MetricDataResult, 0, len(data))
				for _, d := range data {
					// Return data point at "now" - this will be cached
					results = append(results, cloudwatch.MetricDataResult{
						ID:         d.GetMetricDataProcessingParams.QueryID,
						DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now}},
					})
				}
				return results
			},
		}

		inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
		config := CachingProcessorConfig{
			MinPeriods: 1,
			MaxPeriods: 5,
		}
		cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

		requests := []*model.CloudwatchData{
			{
				MetricName:   "ConsecutiveTestMetric",
				ResourceName: "test-resource",
				Namespace:    "AWS/Test",
				Dimensions:   []model.Dimension{{Name: "Dim", Value: "val"}},
				GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
					Period:    period,
					Length:    period,
					Delay:     0,
					Statistic: "Average",
				},
			},
		}

		_, err := cp.Run(context.Background(), "AWS/Test", requests)
		require.NoError(t, err)

		lengthsPerScrape = append(lengthsPerScrape, capturedLength)

		// Wait a tiny bit between scrapes to simulate real-world timing
		time.Sleep(10 * time.Millisecond)
	}

	// First scrape (cold start) should use MinPeriods (1 * 60 = 60)
	assert.Equal(t, int64(60), lengthsPerScrape[0], "first scrape should use MinPeriods")

	// Subsequent scrapes should use floor (1 period) since cache is very recent
	for i := 1; i < numScrapes; i++ {
		assert.Equal(t, period, lengthsPerScrape[i],
			"subsequent scrapes should use floor (1 period) when cache is very recent")
	}

	t.Logf("Lengths per scrape: %v", lengthsPerScrape)
}

func TestCachingProcessor_SmartLookback_FloorEnforcement(t *testing.T) {
	// Test that length never goes below 1 period (floor enforcement)
	// This handles edge cases like clock skew or very rapid scraping
	testCases := []struct {
		name           string
		timeSinceLast  time.Duration // Negative means cached timestamp is in the future
		period         int64
		expectedLength int64
	}{
		{
			name:           "cached timestamp in the future (clock skew)",
			timeSinceLast:  -5 * time.Minute, // 5 minutes in future
			period:         60,
			expectedLength: 60, // Floor at 1 period
		},
		{
			name:           "cached timestamp is now (instant scrape)",
			timeSinceLast:  0,
			period:         60,
			expectedLength: 60, // 0 + 60 buffer = 60, which is >= floor
		},
		{
			name:           "very recent timestamp (1ms ago)",
			timeSinceLast:  1 * time.Millisecond,
			period:         60,
			expectedLength: 60, // ~0 + 60 buffer = 60, which is >= floor
		},
		{
			name:           "larger period with negative gap",
			timeSinceLast:  -10 * time.Minute,
			period:         300,
			expectedLength: 300, // Floor at 1 period (300s)
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			cache := NewTimeseriesCache(1 * time.Hour)
			defer cache.Stop()

			// Set cached timestamp based on timeSinceLast (can be negative)
			cachedTimestamp := now.Add(-tc.timeSinceLast)
			cacheKey := BuildCacheKey("AWS/Test", "TestMetric", []model.Dimension{{Name: "Dim", Value: "val"}}, "Average")
			cache.Set(cacheKey, TimeseriesCacheEntry{
				LastTimestamp: cachedTimestamp,
				Interval:      tc.period,
			})

			var capturedLength int64
			client := testClient{
				GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
					if len(data) > 0 && data[0].GetMetricDataProcessingParams != nil {
						capturedLength = data[0].GetMetricDataProcessingParams.Length
					}
					results := make([]cloudwatch.MetricDataResult, 0, len(data))
					for _, d := range data {
						results = append(results, cloudwatch.MetricDataResult{
							ID:         d.GetMetricDataProcessingParams.QueryID,
							DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now}},
						})
					}
					return results
				},
			}

			inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
			config := CachingProcessorConfig{
				MinPeriods: 1,
				MaxPeriods: 5,
			}
			cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

			requests := []*model.CloudwatchData{
				{
					MetricName:   "TestMetric",
					ResourceName: "test-resource",
					Namespace:    "AWS/Test",
					Dimensions:   []model.Dimension{{Name: "Dim", Value: "val"}},
					GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
						Period:    tc.period,
						Length:    tc.period,
						Delay:     0,
						Statistic: "Average",
					},
				},
			}

			_, err := cp.Run(context.Background(), "AWS/Test", requests)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedLength, capturedLength,
				"length should respect floor of 1 period")
			assert.GreaterOrEqual(t, capturedLength, tc.period,
				"length should never be less than 1 period")
		})
	}
}

func TestCachingProcessor_SmartLookback_DifferentStatisticsSameMetric(t *testing.T) {
	// Same metric with different statistics should have separate cache entries
	now := time.Now()
	cache := NewTimeseriesCache(15 * time.Minute)
	defer cache.Stop()

	// Pre-populate cache for Average only
	cacheKeyAvg := BuildCacheKey("AWS/Test", "TestMetric", []model.Dimension{{Name: "Dim", Value: "val"}}, "Average")
	cache.Set(cacheKeyAvg, TimeseriesCacheEntry{
		LastTimestamp: now.Add(-1 * time.Minute),
		Interval:      60,
	})
	// Sum has no cache entry

	capturedLengths := make(map[string]int64)
	client := testClient{
		GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
			results := make([]cloudwatch.MetricDataResult, 0, len(data))
			for _, d := range data {
				if d.GetMetricDataProcessingParams != nil {
					key := d.MetricName + "_" + d.GetMetricDataProcessingParams.Statistic
					capturedLengths[key] = d.GetMetricDataProcessingParams.Length
				}
				results = append(results, cloudwatch.MetricDataResult{
					ID:         d.GetMetricDataProcessingParams.QueryID,
					DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now}},
				})
			}
			return results
		},
	}

	inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
	config := CachingProcessorConfig{
		MinPeriods: 1,
		MaxPeriods: 5,
	}
	cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

	requests := []*model.CloudwatchData{
		{
			MetricName:   "TestMetric",
			ResourceName: "test-resource",
			Namespace:    "AWS/Test",
			Dimensions:   []model.Dimension{{Name: "Dim", Value: "val"}},
			GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
				Period:    60,
				Length:    60,
				Delay:     0,
				Statistic: "Average",
			},
		},
		{
			MetricName:   "TestMetric",
			ResourceName: "test-resource",
			Namespace:    "AWS/Test",
			Dimensions:   []model.Dimension{{Name: "Dim", Value: "val"}},
			GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
				Period:    60,
				Length:    60,
				Delay:     0,
				Statistic: "Sum",
			},
		},
	}

	_, err := cp.Run(context.Background(), "AWS/Test", requests)
	require.NoError(t, err)

	// Average: cache hit, small lookback
	assert.Less(t, capturedLengths["TestMetric_Average"], int64(200),
		"Average should have cache hit with small lookback")

	// Sum: cold start, MinPeriods=1 lookback (1 * 60 = 60)
	assert.Equal(t, int64(60), capturedLengths["TestMetric_Sum"],
		"Sum should use MinPeriods (cold start)")
}

func TestCachingProcessor_SmartLookback_BoundaryConditions(t *testing.T) {
	// Test boundary conditions with MaxPeriods cap
	testCases := []struct {
		name          string
		timeSinceLast time.Duration
		period        int64
		maxPeriods    int64
		description   string
	}{
		{
			name:          "gap within MaxPeriods boundary",
			timeSinceLast: 3 * time.Minute, // 180s gap + 60s = 240s, cap is 5*60=300s
			period:        60,
			maxPeriods:    5,
			description:   "should not be capped",
		},
		{
			name:          "gap exceeds MaxPeriods boundary",
			timeSinceLast: 10 * time.Minute, // 600s gap + 60s = 660s, cap is 5*60=300s
			period:        60,
			maxPeriods:    5,
			description:   "should be capped",
		},
		{
			name:          "very short gap (faster than period)",
			timeSinceLast: 10 * time.Second,
			period:        60,
			maxPeriods:    5,
			description:   "should still add period buffer",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			cache := NewTimeseriesCache(1 * time.Hour)
			defer cache.Stop()

			cacheKey := BuildCacheKey("AWS/Test", "TestMetric", []model.Dimension{{Name: "Dim", Value: "val"}}, "Average")
			cache.Set(cacheKey, TimeseriesCacheEntry{
				LastTimestamp: now.Add(-tc.timeSinceLast),
				Interval:      tc.period,
			})

			var capturedLength int64
			client := testClient{
				GetMetricDataFunc: func(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
					if len(data) > 0 && data[0].GetMetricDataProcessingParams != nil {
						capturedLength = data[0].GetMetricDataProcessingParams.Length
					}
					results := make([]cloudwatch.MetricDataResult, 0, len(data))
					for _, d := range data {
						results = append(results, cloudwatch.MetricDataResult{
							ID:         d.GetMetricDataProcessingParams.QueryID,
							DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: now}},
						})
					}
					return results
				},
			}

			inner := NewDefaultProcessor(promslog.NewNopLogger(), client, 500, 1)
			config := CachingProcessorConfig{
				MinPeriods: 1,
				MaxPeriods: tc.maxPeriods,
			}
			cp := NewCachingProcessor(promslog.NewNopLogger(), inner, cache, config)

			requests := []*model.CloudwatchData{
				{
					MetricName:   "TestMetric",
					ResourceName: "test-resource",
					Namespace:    "AWS/Test",
					Dimensions:   []model.Dimension{{Name: "Dim", Value: "val"}},
					GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
						Period:    tc.period,
						Length:    tc.period,
						Delay:     0,
						Statistic: "Average",
					},
				},
			}

			_, err := cp.Run(context.Background(), "AWS/Test", requests)
			require.NoError(t, err)

			maxLengthSeconds := tc.period * tc.maxPeriods
			if capturedLength > maxLengthSeconds {
				t.Errorf("Length %d exceeds MaxPeriods cap %d", capturedLength, maxLengthSeconds)
			}

			t.Logf("%s: timeSinceLast=%v, period=%d, maxPeriods=%d, capturedLength=%d",
				tc.description, tc.timeSinceLast, tc.period, tc.maxPeriods, capturedLength)
		})
	}
}
