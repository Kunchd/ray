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

#include "ray/util/logging.h"

namespace ray {
namespace stats {

std::vector<double> BucketHistogramMetric::ComputeBoundaries(int num_buckets,
                                                             double max_value) {
  RAY_CHECK(num_buckets >= 2) << "num_buckets must be >= 2, got " << num_buckets;
  RAY_CHECK(max_value > 0.0) << "max_value must be positive, got " << max_value;
  // Mirrors QuadraticBucketScheme: bucket b spans
  //   [scale * b * (b+1) / 2, scale * (b+1) * (b+2) / 2),
  // where scale = max_value / (num_buckets * (num_buckets - 1) / 2). The OTEL
  // histogram takes upper bounds for buckets 0..num_buckets-2; the implicit
  // (last, +inf) bucket serves as the overflow bucket.
  const double scale = max_value / (num_buckets * (num_buckets - 1) / 2.0);
  std::vector<double> boundaries;
  boundaries.reserve(num_buckets - 1);
  for (int b = 0; b < num_buckets - 1; b++) {
    boundaries.push_back(scale * (b + 1) * (b + 2) / 2.0);
  }
  // Pin the last boundary to exactly max_value to avoid floating point drift.
  boundaries.back() = max_value;
  return boundaries;
}

}  // namespace stats
}  // namespace ray
