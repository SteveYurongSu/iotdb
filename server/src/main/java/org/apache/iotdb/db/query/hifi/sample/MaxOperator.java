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

public class MaxOperator<T extends Number & Comparable<? super T>> extends SampleOperator<T> {

  public MaxOperator(TSDataType type) {
    super(type);
  }

  @Override
  protected int getSampleSizeInSingleBucket() {
    return 1;
  }

  @Override
  protected void sampleFromSingleBucket(List<Long> originalTimestamps, List<T> originalValues,
      List<Long> sampledTimestamps, List<T> sampledValues, final int loIndex, final int hiIndex) {
    long timeCandidate = originalTimestamps.get(hiIndex);
    T valueCandidate = originalValues.get(hiIndex);
    for (int i = loIndex; i < hiIndex; ++i) {
      T value = originalValues.get(i);
      if (valueCandidate.compareTo(value) < 0) {
        valueCandidate = value;
        timeCandidate = originalTimestamps.get(i);
      }
    }
    sampledTimestamps.add(timeCandidate);
    sampledValues.add(valueCandidate);
  }
}