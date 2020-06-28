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

public class EuclidDistanceOperator<T extends Number> implements WeightOperator<T> {

  private final static int INVALID_INDEX = -1;

  private Double weightSum = 0d;
  private long count = 0L;

  @Override
  public void calculate(List<Long> originalTimestamps, List<T> originalValues,
      List<Byte> originalBitmap, List<Double> originalWeights) {
    if (originalValues.size() == 0) {
      return;
    } else if (originalValues.size() == 1) {
      originalWeights.add(SPECIAL_POINT_WEIGHT);
      return;
    } else if (originalValues.size() == 2) {
      originalWeights.add(SPECIAL_POINT_WEIGHT);
      originalWeights.add(SPECIAL_POINT_WEIGHT);
      return;
    }

    // for the first point
    originalWeights.add(SPECIAL_POINT_WEIGHT);

    int lastTimestampIndex = INVALID_INDEX;
    int currentTimestampIndex = getNextValidTimestampIndex(originalBitmap, INVALID_INDEX);
    int nextTimestampIndex = getNextValidTimestampIndex(originalBitmap, currentTimestampIndex);
    while (true) {
      lastTimestampIndex = currentTimestampIndex;
      currentTimestampIndex = nextTimestampIndex;
      nextTimestampIndex = getNextValidTimestampIndex(originalBitmap, currentTimestampIndex);
      if (nextTimestampIndex == INVALID_INDEX) {
        break;
      }
      Double weight = operator(
          originalTimestamps.get(lastTimestampIndex), originalTimestamps.get(currentTimestampIndex),
          originalTimestamps.get(nextTimestampIndex), originalValues.get(lastTimestampIndex),
          originalValues.get(currentTimestampIndex), originalValues.get(nextTimestampIndex));
      originalWeights.add(weight);
      weightSum += weight;
      ++count;
    }

    // for the last point
    originalWeights.add(SPECIAL_POINT_WEIGHT);
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

  private int getNextValidTimestampIndex(List<Byte> originalBitmap, int lastValidIndex) {
    for (int index = lastValidIndex + 1; index < originalBitmap.size(); ++index) {
      if (originalBitmap.get(index).equals((byte) 0B1)) {
        return index;
      }
    }
    return INVALID_INDEX;
  }
}
