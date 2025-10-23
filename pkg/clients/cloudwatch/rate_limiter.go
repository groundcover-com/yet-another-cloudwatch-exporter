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
	// Per-API rate limits
	PerAPILimits map[string]*RateLimit // map[apiName]rateLimit
}

// NewRateLimitedClientFromConfig creates a rate-limited client from config
func NewRateLimitedClientFromConfig(client Client, config RateLimitConfig) Client {
	// If no rate limiting configured, return original client
	if len(config.PerAPILimits) == 0 {
		return client
	}

	rateLimiter, err := NewPerAPIRateLimiter(config.PerAPILimits)
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
	_, err := c.limitAPICalls(ctx, listMetricsCall)
	if err != nil {
		return err
	}
	return c.client.ListMetrics(ctx, namespace, metric, recentlyActiveOnly, fn)
}

func (c *simpleRateLimitedClient) GetMetricData(ctx context.Context, getMetricData []*model.CloudwatchData, namespace string, startTime time.Time, endTime time.Time) []MetricDataResult {
	_, err := c.limitAPICalls(ctx, getMetricDataCall)
	if err != nil {
		return nil
	}
	return c.client.GetMetricData(ctx, getMetricData, namespace, startTime, endTime)
}

func (c *simpleRateLimitedClient) GetMetricStatistics(ctx context.Context, logger *slog.Logger, dimensions []model.Dimension, namespace string, metric *model.MetricConfig) []*model.Datapoint {
	_, err := c.limitAPICalls(ctx, getMetricStatisticsCall)
	if err != nil {
		return nil
	}
	return c.client.GetMetricStatistics(ctx, logger, dimensions, namespace, metric)
}

func (c *simpleRateLimitedClient) limitAPICalls(ctx context.Context, apiName string) (bool, error) {
	if c.rateLimiter.Allow(apiName) {
		promutil.CloudwatchRateLimitAllowedCounter.WithLabelValues(apiName).Inc()
		return true, nil
	}
	promutil.CloudwatchRateLimitWaitCounter.WithLabelValues(apiName).Inc()
	if err := c.rateLimiter.Wait(ctx, apiName); err != nil {
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
