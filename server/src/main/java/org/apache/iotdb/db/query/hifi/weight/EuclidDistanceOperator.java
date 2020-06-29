/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.hifi.weight;

import java.util.List;

public class EuclidDistanceOperator<T extends Number & Comparable<? super T>> implements
    WeightOperator<T> {

  private Double weightSum = 0d;
  private long count = 0L;

  @Override
  public void calculate(List<Long> timestamps, List<T> values, List<Double> weights) {
    if (values.size() == 0) {
      return;
    } else if (values.size() == 1) {
      weights.add(SPECIAL_POINT_WEIGHT);
      return;
    }

    // for the first point
    weights.add(SPECIAL_POINT_WEIGHT);

    for (int i = 1; i < values.size() - 1; ++i) {
      Double weight = operator(timestamps.get(i - 1), timestamps.get(i), timestamps.get(i + 1),
          values.get(i - 1), values.get(i), values.get(i + 1));
      weights.add(weight);
      weightSum += weight;
      ++count;
    }

    // for the last point
    weights.add(SPECIAL_POINT_WEIGHT);
  }

  @Override
  public Double operator(Long time0, Long time1, Long time2, T value0, T value1, T value2) {
    long deltaT0 = time1 - time0;
    long deltaT1 = time2 - time1;
    double double1 = value1.doubleValue();
    double deltaV0 = double1 - value0.doubleValue();
    double deltaV1 = value2.doubleValue() - double1;
    return Math.sqrt(deltaT0 * deltaT0 + deltaV0 * deltaV0)
        + Math.sqrt(deltaT1 * deltaT1 + deltaV1 * deltaV1);
  }

  @Override
  public Double getCurrentAverageWeight() {
    return weightSum / count;
  }
}
