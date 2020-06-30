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

public class M4Operator<T extends Number & Comparable<? super T>> extends SampleOperator<T> {

  public M4Operator(TSDataType type) {
    super(type);
  }

  @Override
  protected int getSampleSizeInSingleBucket() {
    return 4;
  }

  @Override
  protected void sampleFromSingleBucket(List<Long> originalTimestamps, List<T> originalValues,
      List<Long> sampledTimestamps, List<T> sampledValues, final int loIndex, final int hiIndex) {
    if (loIndex == hiIndex) {
      sampledTimestamps.add(originalTimestamps.get(loIndex));
      sampledValues.add(originalValues.get(loIndex));
      return;
    }

    int minIndex = INVALID_INDEX;
    int maxIndex = INVALID_INDEX;
    T minValue = null;
    T maxValue = null;
    for (int i = loIndex + 1; i < hiIndex; ++i) {
      T candidate = originalValues.get(i);
      if (minValue == null || candidate.compareTo(minValue) < 0) {
        minIndex = i;
        minValue = candidate;
      }
      if (maxValue == null || maxValue.compareTo(candidate) < 0) {
        maxIndex = i;
        maxValue = candidate;
      }
    }

    sampledTimestamps.add(originalTimestamps.get(loIndex));
    sampledValues.add(originalValues.get(loIndex));

    int smallerIndex = Math.min(minIndex, maxIndex);
    int biggerIndex = Math.max(minIndex, maxIndex);
    if (smallerIndex != INVALID_INDEX) {
      sampledTimestamps.add(originalTimestamps.get(smallerIndex));
      sampledValues.add(originalValues.get(smallerIndex));
      if (biggerIndex != smallerIndex) {
        sampledTimestamps.add(originalTimestamps.get(biggerIndex));
        sampledValues.add(originalValues.get(biggerIndex));
      }
    }

    sampledTimestamps.add(originalTimestamps.get(hiIndex));
    sampledValues.add(originalValues.get(hiIndex));
  }
}
