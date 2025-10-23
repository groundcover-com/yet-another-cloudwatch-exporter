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

// NewSingleAPIRateLimiter creates a rate limiter for a single API
func NewSingleAPIRateLimiter(apiName string, rateLimit *RateLimit) (RateLimiter, error) {
	if rateLimit == nil {
		return nil, nil
	}

	fmt.Printf("[DEBUG] Rate limiter: Creating %s rate limiter: %.2f requests/second (%d per %v)\n",
		apiName, float64(rateLimit.Count)/rateLimit.Duration.Seconds(), rateLimit.Count, rateLimit.Duration)

	limits := map[string]*RateLimit{
		apiName: rateLimit,
	}
	return NewPerAPIRateLimiter(limits)
}

// RateLimitConfig holds actual rate limiter instances for CloudWatch APIs
type RateLimitConfig struct {
	// Per-API rate limiters (already created and ready to use)
	PerAPILimiters map[string]RateLimiter // map[apiName]rateLimiter
}

// NewRateLimitedClientFromConfig creates a rate-limited client from config
func NewRateLimitedClientFromConfig(client Client, config RateLimitConfig) Client {
	// Check if any rate limiters are configured
	if len(config.PerAPILimiters) == 0 {
		// No rate limiting configured - return original client unchanged
		return client
	}

	// Create a combined rate limiter from all the individual API rate limiters
	rateLimiter := &combinedRateLimiter{limiters: config.PerAPILimiters}
	return &SimpleRateLimitedClient{Client: client, RateLimiter: rateLimiter}
}

// SimpleRateLimitedClient is a minimal wrapper that only adds rate limiting
type SimpleRateLimitedClient struct {
	Client      Client
	RateLimiter RateLimiter
}

func (c *SimpleRateLimitedClient) ListMetrics(ctx context.Context, namespace string, metric *model.MetricConfig, recentlyActiveOnly bool, fn func(page []*model.Metric)) error {
	_, err := c.limitAPICalls(ctx, listMetricsCall)
	if err != nil {
		return err
	}
	return c.Client.ListMetrics(ctx, namespace, metric, recentlyActiveOnly, fn)
}

func (c *SimpleRateLimitedClient) GetMetricData(ctx context.Context, getMetricData []*model.CloudwatchData, namespace string, startTime time.Time, endTime time.Time) []MetricDataResult {
	_, err := c.limitAPICalls(ctx, getMetricDataCall)
	if err != nil {
		return nil
	}
	return c.Client.GetMetricData(ctx, getMetricData, namespace, startTime, endTime)
}

func (c *SimpleRateLimitedClient) GetMetricStatistics(ctx context.Context, logger *slog.Logger, dimensions []model.Dimension, namespace string, metric *model.MetricConfig) []*model.Datapoint {
	_, err := c.limitAPICalls(ctx, getMetricStatisticsCall)
	if err != nil {
		return nil
	}
	return c.Client.GetMetricStatistics(ctx, logger, dimensions, namespace, metric)
}

func (c *SimpleRateLimitedClient) limitAPICalls(ctx context.Context, apiName string) (bool, error) {
	if c.RateLimiter.Allow(apiName) {
		promutil.CloudwatchRateLimitAllowedCounter.WithLabelValues(apiName).Inc()
		return true, nil
	}
	promutil.CloudwatchRateLimitWaitCounter.WithLabelValues(apiName).Inc()
	if err := c.RateLimiter.Wait(ctx, apiName); err != nil {
		return false, err
	}
	return true, nil // After waiting, the call should proceed
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

// perAPIRateLimiter applies different rate limits per API
type perAPIRateLimiter struct {
	limiters map[string]*rate.Limiter
}

// combinedRateLimiter combines multiple individual API rate limiters
type combinedRateLimiter struct {
	limiters map[string]RateLimiter
}

// NewPerAPIRateLimiter creates a rate limiter with different limits per API
func NewPerAPIRateLimiter(apiLimits map[string]*RateLimit) (RateLimiter, error) {
	limiters := make(map[string]*rate.Limiter)

	for apiName, rateLimit := range apiLimits {
		if rateLimit != nil {
			rateLimitValue, burst, err := rateLimitToLimiter(rateLimit)
			if err != nil {
				return nil, fmt.Errorf("invalid %s rate limit: %w", apiName, err)
			}
			limiters[apiName] = rate.NewLimiter(rateLimitValue, burst)
		}
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

func (c *combinedRateLimiter) Allow(apiName string) bool {
	if limiter, exists := c.limiters[apiName]; exists {
		return limiter.Allow(apiName)
	}
	return true // No limit for this API
}

func (c *combinedRateLimiter) Wait(ctx context.Context, apiName string) error {
	if limiter, exists := c.limiters[apiName]; exists {
		return limiter.Wait(ctx, apiName)
	}
	return nil // No limit for this API
}
