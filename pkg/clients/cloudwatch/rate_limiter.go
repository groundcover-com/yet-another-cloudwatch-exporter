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
package cloudwatch

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/time/rate"

	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/model"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/promutil"
)

// RateLimit represents a rate limiting configuration
type RateLimit struct {
	Count    int           // Number of requests allowed
	Duration time.Duration // Time period for the count
}

// RateLimitConfig holds rate limiting configuration for CloudWatch APIs
type RateLimitConfig struct {
	// Single rate limit applied to all APIs (if PerAPILimits is empty)
	SingleLimit *RateLimit

	// Per-API rate limits (takes precedence over SingleLimit if set)
	PerAPILimits map[string]*RateLimit // map[apiName]rateLimit
}

// NewRateLimitedClientFromConfig creates a rate-limited client from config
func NewRateLimitedClientFromConfig(client Client, config RateLimitConfig) Client {
	// If no rate limiting configured, return original client
	if config.SingleLimit == nil && len(config.PerAPILimits) == 0 {
		return client
	}

	// Use per-API limits if available, otherwise fall back to single limit
	if len(config.PerAPILimits) > 0 {
		rateLimiter, err := NewPerAPIRateLimiter(
			config.PerAPILimits[listMetricsCall],
			config.PerAPILimits[getMetricDataCall],
			config.PerAPILimits[getMetricStatisticsCall],
		)
		if err != nil {
			return client // Return original client if config is invalid
		}
		return &simpleRateLimitedClient{client: client, rateLimiter: rateLimiter}
	}

	// Use single rate limit for all APIs
	rateLimiter, err := NewSingleRateLimiter(config.SingleLimit)
	if err != nil {
		return client // Return original client if config is invalid
	}

	return &simpleRateLimitedClient{client: client, rateLimiter: rateLimiter}
}

// simpleRateLimitedClient is a minimal wrapper that only adds rate limiting
type simpleRateLimitedClient struct {
	client      Client
	rateLimiter RateLimiter
}

func (c *simpleRateLimitedClient) ListMetrics(ctx context.Context, namespace string, metric *model.MetricConfig, recentlyActiveOnly bool, fn func(page []*model.Metric)) error {
	// Check if request would be allowed immediately
	if c.rateLimiter.Allow(listMetricsCall) {
		promutil.CloudwatchRateLimitAllowedCounter.WithLabelValues(listMetricsCall).Inc()
	} else {
		// Request will be rate limited
		promutil.CloudwatchRateLimitWaitCounter.WithLabelValues(listMetricsCall).Inc()
		if err := c.rateLimiter.Wait(ctx, listMetricsCall); err != nil {
			return err
		}
	}
	return c.client.ListMetrics(ctx, namespace, metric, recentlyActiveOnly, fn)
}

func (c *simpleRateLimitedClient) GetMetricData(ctx context.Context, getMetricData []*model.CloudwatchData, namespace string, startTime time.Time, endTime time.Time) []MetricDataResult {
	// Check if request would be allowed immediately
	if c.rateLimiter.Allow(getMetricDataCall) {
		promutil.CloudwatchRateLimitAllowedCounter.WithLabelValues(getMetricDataCall).Inc()
	} else {
		// Request will be rate limited
		promutil.CloudwatchRateLimitWaitCounter.WithLabelValues(getMetricDataCall).Inc()
		if err := c.rateLimiter.Wait(ctx, getMetricDataCall); err != nil {
			return nil
		}
	}
	return c.client.GetMetricData(ctx, getMetricData, namespace, startTime, endTime)
}

func (c *simpleRateLimitedClient) GetMetricStatistics(ctx context.Context, logger *slog.Logger, dimensions []model.Dimension, namespace string, metric *model.MetricConfig) []*model.Datapoint {
	// Check if request would be allowed immediately
	if c.rateLimiter.Allow(getMetricStatisticsCall) {
		promutil.CloudwatchRateLimitAllowedCounter.WithLabelValues(getMetricStatisticsCall).Inc()
	} else {
		// Request will be rate limited
		promutil.CloudwatchRateLimitWaitCounter.WithLabelValues(getMetricStatisticsCall).Inc()
		if err := c.rateLimiter.Wait(ctx, getMetricStatisticsCall); err != nil {
			return nil
		}
	}
	return c.client.GetMetricStatistics(ctx, logger, dimensions, namespace, metric)
}

// rateLimitToLimiter converts a RateLimit to rate.Limit and burst
func rateLimitToLimiter(rl *RateLimit) (rate.Limit, int, error) {
	if rl == nil {
		return rate.Inf, 1, nil // No limit
	}

	if rl.Count <= 0 {
		return 0, 0, fmt.Errorf("rate limit count must be positive, got %d", rl.Count)
	}

	if rl.Duration <= 0 {
		return 0, 0, fmt.Errorf("rate limit duration must be positive, got %v", rl.Duration)
	}

	ratePerSecond := float64(rl.Count) / rl.Duration.Seconds()
	return rate.Limit(ratePerSecond), rl.Count, nil
}

// RateLimiter interface for rate limiting API calls
type RateLimiter interface {
	Wait(ctx context.Context, apiName string) error
	Allow(apiName string) bool
}

// singleRateLimiter applies the same rate limit to all API calls
type singleRateLimiter struct {
	limiter *rate.Limiter
}

// NewSingleRateLimiter creates a rate limiter that applies the same limit to all APIs
func NewSingleRateLimiter(rl *RateLimit) (RateLimiter, error) {
	rateLimit, burst, err := rateLimitToLimiter(rl)
	if err != nil {
		return nil, err
	}
	return &singleRateLimiter{
		limiter: rate.NewLimiter(rateLimit, burst),
	}, nil
}

func (s *singleRateLimiter) Wait(ctx context.Context, apiName string) error {
	return s.limiter.Wait(ctx)
}

func (s *singleRateLimiter) Allow(apiName string) bool {
	return s.limiter.Allow()
}

// perAPIRateLimiter applies different rate limits per API
type perAPIRateLimiter struct {
	limiters map[string]*rate.Limiter
}

// NewPerAPIRateLimiter creates a rate limiter with different limits per API
func NewPerAPIRateLimiter(listMetrics, getMetricData, getMetricStatistics *RateLimit) (RateLimiter, error) {
	limiters := make(map[string]*rate.Limiter)

	// Create limiter for ListMetrics
	if listMetrics != nil {
		rateLimit, burst, err := rateLimitToLimiter(listMetrics)
		if err != nil {
			return nil, fmt.Errorf("invalid ListMetrics rate limit: %w", err)
		}
		limiters[listMetricsCall] = rate.NewLimiter(rateLimit, burst)
	}

	// Create limiter for GetMetricData
	if getMetricData != nil {
		rateLimit, burst, err := rateLimitToLimiter(getMetricData)
		if err != nil {
			return nil, fmt.Errorf("invalid GetMetricData rate limit: %w", err)
		}
		limiters[getMetricDataCall] = rate.NewLimiter(rateLimit, burst)
	}

	// Create limiter for GetMetricStatistics
	if getMetricStatistics != nil {
		rateLimit, burst, err := rateLimitToLimiter(getMetricStatistics)
		if err != nil {
			return nil, fmt.Errorf("invalid GetMetricStatistics rate limit: %w", err)
		}
		limiters[getMetricStatisticsCall] = rate.NewLimiter(rateLimit, burst)
	}

	return &perAPIRateLimiter{limiters: limiters}, nil
}

func (p *perAPIRateLimiter) Wait(ctx context.Context, apiName string) error {
	if limiter, exists := p.limiters[apiName]; exists {
		return limiter.Wait(ctx)
	}
	return nil // No rate limiting for this API
}

func (p *perAPIRateLimiter) Allow(apiName string) bool {
	if limiter, exists := p.limiters[apiName]; exists {
		return limiter.Allow()
	}
	return true // No rate limiting for this API
}
