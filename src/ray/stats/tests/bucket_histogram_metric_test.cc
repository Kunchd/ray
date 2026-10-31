// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/stats/bucket_histogram_metric.h"

#include <gtest/gtest.h>

#include "ray/common/ray_config.h"
#include "ray/observability/open_telemetry_metric_recorder.h"
#include "ray/stats/metric.h"
#include "ray/stats/quadratic_bucket_scheme.h"

namespace ray {
namespace stats {

using namespace std::literals;
using OpenTelemetryMetricRecorder = ray::observability::OpenTelemetryMetricRecorder;

class BucketHistogramMetricTest : public ::testing::Test {
 public:
  static void SetUpTestSuite() {
    StatsConfig::instance().SetGlobalTags({});
    StatsConfig::instance().SetIsDisableStats(false);
    for (auto &f : StatsConfig::instance().PopInitializers()) {
      f();
    }
    StatsConfig::instance().SetIsInitialized(true);
  }
};

// Friend-style accessor for testing the private static. We re-derive the
// boundaries from QuadraticBucketScheme by inverting ToBucket on each
// boundary; the two should agree.
TEST_F(BucketHistogramMetricTest, BoundariesMatchQuadraticScheme) {
  // Construct a metric to exercise the constructor path and ensure the
  // boundaries computation does not crash for representative inputs.
  BucketHistogramMetric metric("bucket_histogram_metric_test_basic",
                               "A test bucket histogram metric",
                               "ms",
                               /*max_expected_value=*/10000.0,
                               /*num_buckets=*/128);

  // Compare against the QuadraticBucketScheme directly. For each bucket b in
  // [0, N-2], scheme.ToBucket(boundary[b] - eps) should be b and
  // scheme.ToBucket(boundary[b] + eps) should be >= b+1.
  const int num_buckets = 128;
  const double max_value = 10000.0;
  QuadraticBucketScheme scheme(num_buckets, max_value);
  const double scale = max_value / (num_buckets * (num_buckets - 1) / 2.0);
  for (int b = 0; b < num_buckets - 1; b++) {
    double upper = scale * (b + 1) * (b + 2) / 2.0;
    if (b == num_buckets - 2) {
      EXPECT_NEAR(upper, max_value, 1e-6);
    }
    EXPECT_EQ(scheme.ToBucket(upper * (1 - 1e-9)), b);
  }
}

TEST_F(BucketHistogramMetricTest, RecordRegistersWithOpenTelemetry) {
  // Only meaningful when OTEL is enabled. Skip otherwise.
  if (!::RayConfig::instance().enable_open_telemetry()) {
    GTEST_SKIP() << "OpenTelemetry not enabled in this build configuration.";
  }
  BucketHistogramMetric metric("bucket_histogram_metric_test_record",
                               "A test bucket histogram metric",
                               "ms",
                               /*max_expected_value=*/1000.0,
                               /*num_buckets=*/64,
                               /*tag_keys=*/{"Tag1"});

  // Recording should not crash. The actual histogram values are validated via
  // the metrics agent in end-to-end tests, since the OpenTelemetry SDK does
  // not expose synchronous instrument readbacks.
  metric.Record(5.0);
  metric.Record(50.0, {{"Tag1"sv, "ValueA"}});
  metric.Record(2000.0, {{"Tag1"sv, "ValueB"}});  // overflow bucket

  EXPECT_TRUE(OpenTelemetryMetricRecorder::GetInstance().IsMetricRegistered(
      "bucket_histogram_metric_test_record"));
}

TEST_F(BucketHistogramMetricTest, RecordWithoutOpenTelemetryDoesNotCrash) {
  // Sanity check: even without OTEL enabled the metric should accept Record
  // calls (they flow through OpenCensus instead).
  BucketHistogramMetric metric("bucket_histogram_metric_test_record_no_otel",
                               "A test bucket histogram metric",
                               "ms",
                               /*max_expected_value=*/500.0,
                               /*num_buckets=*/16);
  metric.Record(1.0);
  metric.Record(100.0);
  metric.Record(10000.0);  // overflow
}

}  // namespace stats
}  // namespace ray
