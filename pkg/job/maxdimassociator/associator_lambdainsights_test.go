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
package maxdimassociator

import (
	"testing"

	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/require"

	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/model"
)

var lambdaInsightsFunction = &model.TaggedResource{
	ARN:       "arn:aws:lambda:us-east-2:123456789012:function:cpu-intensive",
	Namespace: "LambdaInsights",
}

var lambdaInsightsFunctionWithVersion = &model.TaggedResource{
	ARN:       "arn:aws:lambda:us-east-2:123456789012:function:my-function:v1",
	Namespace: "LambdaInsights",
}

var lambdaInsightsResources = []*model.TaggedResource{
	lambdaInsightsFunction,
	lambdaInsightsFunctionWithVersion,
}

func TestAssociatorLambdaInsights(t *testing.T) {
	type args struct {
		dimensionRegexps []model.DimensionsRegexp
		resources        []*model.TaggedResource
		metric           *model.Metric
	}

	type testCase struct {
		name             string
		args             args
		expectedSkip     bool
		expectedResource *model.TaggedResource
	}

	testcases := []testCase{
		{
			name: "should match with function_name dimension",
			args: args{
				dimensionRegexps: config.SupportedServices.GetService("LambdaInsights").ToModelDimensionsRegexp(),
				resources:        lambdaInsightsResources,
				metric: &model.Metric{
					MetricName: "memory_utilization",
					Namespace:  "LambdaInsights",
					Dimensions: []model.Dimension{
						{Name: "function_name", Value: "cpu-intensive"},
					},
				},
			},
			expectedSkip:     false,
			expectedResource: lambdaInsightsFunction,
		},
		{
			name: "should match with function_name and version dimensions",
			args: args{
				dimensionRegexps: config.SupportedServices.GetService("LambdaInsights").ToModelDimensionsRegexp(),
				resources:        lambdaInsightsResources,
				metric: &model.Metric{
					MetricName: "cpu_total_time",
					Namespace:  "LambdaInsights",
					Dimensions: []model.Dimension{
						{Name: "function_name", Value: "my-function"},
						{Name: "version", Value: "v1"},
					},
				},
			},
			expectedSkip:     false,
			expectedResource: lambdaInsightsFunctionWithVersion,
		},
		{
			name: "should skip with unmatched function_name",
			args: args{
				dimensionRegexps: config.SupportedServices.GetService("LambdaInsights").ToModelDimensionsRegexp(),
				resources:        lambdaInsightsResources,
				metric: &model.Metric{
					MetricName: "memory_utilization",
					Namespace:  "LambdaInsights",
					Dimensions: []model.Dimension{
						{Name: "function_name", Value: "unknown-function"},
					},
				},
			},
			expectedSkip:     true,
			expectedResource: nil,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			associator := NewAssociator(promslog.NewNopLogger(), tc.args.dimensionRegexps, tc.args.resources)
			res, skip := associator.AssociateMetricToResource(tc.args.metric)
			require.Equal(t, tc.expectedSkip, skip)
			require.Equal(t, tc.expectedResource, res)
		})
	}
}
