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
package job_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/clients/account"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/clients/tagging"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/job"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/job/getmetricdata"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/model"
)

// A cluster name reused across regions inside one integration collided in the
// shared cache: the first region to answer advanced LastTimestamp, and every
// other region's point at that timestamp was then dropped as already seen.
func TestScrapeAwsData_SameDimensionsAcrossScopesSharingOneCache(t *testing.T) {
	type scope struct{ accountID, region string }
	jobs := []struct {
		Name    string
		jobsCfg func(region string) model.JobsConfig
	}{
		{Name: "discovery job", jobsCfg: rdsDiscoveryJob},
		{Name: "custom namespace job", jobsCfg: rdsCustomNamespaceJob},
	}
	scopes := []struct {
		Name   string
		first  scope
		second scope
	}{
		{
			Name:   "two regions scraping the same cluster name in one account each keep their own data point",
			first:  scope{accountID: "111111111111", region: "region-a"},
			second: scope{accountID: "111111111111", region: "region-b"},
		},
		{
			Name:   "two accounts scraping the same cluster name in one region each keep their own data point",
			first:  scope{accountID: "111111111111", region: "region-a"},
			second: scope{accountID: "222222222222", region: "region-a"},
		},
	}
	for _, j := range jobs {
		for _, s := range scopes {
			t.Run(j.Name+"/"+s.Name, func(t *testing.T) {
				sample := time.Now().Add(-2 * time.Minute)
				cache := getmetricdata.NewTimeseriesCache()
				defer cache.Stop()

				cachingConfig := getmetricdata.DefaultCachingProcessorConfig()
				cachingConfig.KeyPrefix = "integration-1"

				// Each scope is scraped by its own call so the first one has
				// written to the cache before the second one reads it; scraping
				// both regions in one call races and can hide the collision.
				scrape := func(sc scope) []model.DataPoint {
					factory := &sharedClusterFactory{accountID: sc.accountID, sample: sample}
					_, cwData := job.ScrapeAwsData(
						context.Background(),
						promslog.NewNopLogger(),
						j.jobsCfg(sc.region),
						factory,
						500,
						cloudwatch.ConcurrencyConfig{SingleLimit: 1, ListMetrics: 1, GetMetricData: 1, GetMetricStatistics: 1},
						nil,
						1,
						nil,
						cache,
						cachingConfig,
					)
					require.Len(t, cwData, 1)
					require.Len(t, cwData[0].Data, 1)
					require.NotNil(t, cwData[0].Data[0].GetMetricDataResult)
					return cwData[0].Data[0].GetMetricDataResult.DataPoints
				}

				first := scrape(s.first)
				second := scrape(s.second)

				require.Len(t, first, 1)
				require.Len(t, second, 1, "the second scope's point must not be deduplicated against the first scope's")
				assert.Equal(t, sample, second[0].Timestamp)
			})
		}
	}
}

var sharedClusterMetric = &model.MetricConfig{
	Name:       "CPUUtilization",
	Statistics: []string{"Average"},
	Period:     60,
	Length:     300,
}

func rdsDiscoveryJob(region string) model.JobsConfig {
	return model.JobsConfig{
		DiscoveryJobs: []model.DiscoveryJob{{
			Regions:           []string{region},
			Namespace:         "AWS/RDS",
			Roles:             []model.Role{{}},
			Metrics:           []*model.MetricConfig{sharedClusterMetric},
			DimensionsRegexps: config.SupportedServices.GetService("AWS/RDS").ToModelDimensionsRegexp(),
		}},
	}
}

func rdsCustomNamespaceJob(region string) model.JobsConfig {
	return model.JobsConfig{
		CustomNamespaceJobs: []model.CustomNamespaceJob{{
			Regions:   []string{region},
			Name:      "rds",
			Namespace: "AWS/RDS",
			Roles:     []model.Role{{}},
			Metrics:   []*model.MetricConfig{sharedClusterMetric},
		}},
	}
}

// sharedClusterFactory serves one account in which every region has an RDS
// cluster named cluster-a, each returning a single point at sample.
type sharedClusterFactory struct {
	accountID string
	sample    time.Time
}

func (f *sharedClusterFactory) GetCloudwatchClient(string, string, model.Role, cloudwatch.ConcurrencyConfig, *cloudwatch.GlobalRateLimiter) cloudwatch.Client {
	return sharedClusterCloudwatchClient{sample: f.sample}
}

func (f *sharedClusterFactory) GetTaggingClient(region string, _ model.Role, _ int) tagging.Client {
	return sharedClusterTaggingClient{accountID: f.accountID, region: region}
}

func (f *sharedClusterFactory) GetAccountClient(string, model.Role) account.Client {
	return staticAccountClient{accountID: f.accountID}
}

type staticAccountClient struct{ accountID string }

func (c staticAccountClient) GetAccount(context.Context) (string, error) { return c.accountID, nil }

func (c staticAccountClient) GetAccountAlias(context.Context) (string, error) { return "", nil }

type sharedClusterTaggingClient struct{ accountID, region string }

func (c sharedClusterTaggingClient) GetResources(context.Context, model.DiscoveryJob, string) ([]*model.TaggedResource, error) {
	return []*model.TaggedResource{{
		ARN:       "arn:aws:rds:" + c.region + ":" + c.accountID + ":cluster:cluster-a",
		Namespace: "AWS/RDS",
		Region:    c.region,
	}}, nil
}

type sharedClusterCloudwatchClient struct{ sample time.Time }

func (c sharedClusterCloudwatchClient) ListMetrics(_ context.Context, _ string, metric *model.MetricConfig, _ bool, fn func(page []*model.Metric)) error {
	fn([]*model.Metric{{
		MetricName: metric.Name,
		Namespace:  "AWS/RDS",
		Dimensions: []model.Dimension{
			{Name: "DBClusterIdentifier", Value: "cluster-a"},
			{Name: "Role", Value: "WRITER"},
		},
	}})
	return nil
}

func (c sharedClusterCloudwatchClient) GetMetricData(_ context.Context, data []*model.CloudwatchData, _ string, _ time.Time, _ time.Time) []cloudwatch.MetricDataResult {
	results := make([]cloudwatch.MetricDataResult, 0, len(data))
	for _, d := range data {
		results = append(results, cloudwatch.MetricDataResult{
			ID:         d.GetMetricDataProcessingParams.QueryID,
			DataPoints: []cloudwatch.DataPoint{{Value: aws.Float64(42), Timestamp: c.sample}},
		})
	}
	return results
}

func (c sharedClusterCloudwatchClient) GetMetricStatistics(context.Context, *slog.Logger, []model.Dimension, string, *model.MetricConfig) []*model.MetricStatisticsResult {
	return nil
}
