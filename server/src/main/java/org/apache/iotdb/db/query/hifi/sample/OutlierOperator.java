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

package org.apache.iotdb.db.query.hifi.sample;

import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class OutlierOperator<T extends Number & Comparable<? super T>> extends SampleOperator<T> {

  public OutlierOperator(TSDataType type) {
    super(type);
  }

  @Override
  protected int getSampleSizeInSingleBucket() {
    return 1;
  }

  @Override
  protected void sampleFromSingleBucket(List<Long> originalTimestamps, List<T> originalValues,
      List<Long> sampledTimestamps, List<T> sampledValues, final int loIndex, final int hiIndex) {
    long firstPointTime = originalTimestamps.get(loIndex);
    double firstPointValue = originalValues.get(loIndex).doubleValue();
    long lastPointTime = originalTimestamps.get(hiIndex);
    double lastPointValue = originalValues.get(hiIndex).doubleValue();
    double a = lastPointValue - firstPointValue;
    double b = lastPointTime - firstPointTime;
    double c = firstPointTime * lastPointValue - lastPointTime * firstPointValue;
    double maxDistanceSquare = 0.0d;
    long timeCandidate = originalTimestamps.get(loIndex);
    T valueCandidate = originalValues.get(loIndex);
    for (int i = loIndex + 1; i < hiIndex; ++i) {
      long x = originalTimestamps.get(i);
      T y = originalValues.get(i);
      double numerator = a * x + b * y.doubleValue() + c;
      double distanceSquare = numerator * numerator / (a * a + b * b);
      if (maxDistanceSquare < distanceSquare) {
        timeCandidate = x;
        valueCandidate = y;
      }
    }
    sampledTimestamps.add(timeCandidate);
    sampledValues.add(valueCandidate);
  }
}
