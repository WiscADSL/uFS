// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CFS_UTIL_HISTOGRAM_H_
#define CFS_UTIL_HISTOGRAM_H_

#include <string>

namespace PerfUtils {

class Histogram {
public:
  Histogram() {}
  ~Histogram() {}

  void Clear();
  void Add(double value);
  void Merge(const Histogram &other);

  std::string ToString() const;

private:
  enum { kNumBuckets = 154 };

  double Median() const;
  double Percentile(double p) const;
  double Average() const;
  double StandardDeviation() const;

  static const double kBucketLimit[kNumBuckets];

  double min_;
  double max_;
  double num_;
  double sum_;
  double sum_squares_;

  double buckets_[kNumBuckets];
};

} // namespace PerfUtils

#endif // CFS_UTIL_HISTOGRAM_H_