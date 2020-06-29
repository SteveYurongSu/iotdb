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
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class SampleOperator<T extends Number & Comparable<? super T>> {

  protected static final int INVALID_INDEX = -1;

  public static SampleOperator<?> getSampleOperator(String name, TSDataType type) {
    switch (name.toLowerCase()) {
      case "m4":
        switch (type) {
          case INT32:
            new M4Operator<Integer>();
          case INT64:
            new M4Operator<Long>();
          case FLOAT:
            new M4Operator<Float>();
          case DOUBLE:
            new M4Operator<Double>();
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      default:
        throw new UnsupportedOperationException("Sample operator \"" + name + "\" is unsupported.");
    }
  }

  /**
   * @param bucketWeight The weight of the bucket when only one point is sampled
   */
  public abstract void sample(List<Long> originalTimestamps, List<T> originalValues,
      List<Double> originalWeights, List<Long> sampledTimestamps, List<T> sampledValues,
      double bucketWeight);

  protected abstract void sampleFromSingleBucket(List<Long> originalTimestamps,
      List<T> originalValues, List<Long> sampledTimestamps, List<T> sampledValues,
      final int loIndex, final int hiIndex);

  protected int getNextHiIndex(List<Double> weights, final double bucketWeight,
      final int lastHiIndex) {
    if (weights.size() - 1 <= lastHiIndex) {
      return INVALID_INDEX;
    }
    int index = lastHiIndex + 1;
    double currentBucketWeight = 0;
    while (index < weights.size()) {
      currentBucketWeight += weights.get(index);
      if (bucketWeight <= currentBucketWeight) {
        return index;
      }
      ++index;
    }
    return weights.size() - 1;
  }
}
