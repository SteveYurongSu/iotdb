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
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class SampleOperator<T extends Number & Comparable<? super T>> {

  protected static final int INVALID_INDEX = -1;

  protected TSDataType type;

  public SampleOperator(TSDataType type) {
    this.type = type;
  }

  public static SampleOperator<?> getSampleOperator(String name, TSDataType type) {
    switch (name.toLowerCase()) {
      case SQLConstant.M4_OPERATOR:
        switch (type) {
          case INT32:
            new M4Operator<Integer>(type);
          case INT64:
            new M4Operator<Long>(type);
          case FLOAT:
            new M4Operator<Float>(type);
          case DOUBLE:
            new M4Operator<Double>(type);
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      case SQLConstant.RANDOM_OPERATOR:
        switch (type) {
          case INT32:
            new RandomOperator<Integer>(type);
          case INT64:
            new RandomOperator<Long>(type);
          case FLOAT:
            new RandomOperator<Float>(type);
          case DOUBLE:
            new RandomOperator<Double>(type);
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      case SQLConstant.AVERAGE_OPERATOR:
        switch (type) {
          case INT32:
            new AverageOperator<Integer>(type);
          case INT64:
            new AverageOperator<Long>(type);
          case FLOAT:
            new AverageOperator<Float>(type);
          case DOUBLE:
            new AverageOperator<Double>(type);
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      case SQLConstant.MAX_OPERATOR:
        switch (type) {
          case INT32:
            new MaxOperator<Integer>(type);
          case INT64:
            new MaxOperator<Long>(type);
          case FLOAT:
            new MaxOperator<Float>(type);
          case DOUBLE:
            new MaxOperator<Double>(type);
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      case SQLConstant.MIN_OPERATOR:
        switch (type) {
          case INT32:
            new MinOperator<Integer>(type);
          case INT64:
            new MinOperator<Long>(type);
          case FLOAT:
            new MinOperator<Float>(type);
          case DOUBLE:
            new MinOperator<Double>(type);
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      case SQLConstant.OUTLIER_OPERATOR:
        switch (type) {
          case INT32:
            new OutlierOperator<Integer>(type);
          case INT64:
            new OutlierOperator<Long>(type);
          case FLOAT:
            new OutlierOperator<Float>(type);
          case DOUBLE:
            new OutlierOperator<Double>(type);
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
  public void sample(List<Long> originalTimestamps, List<T> originalValues,
      List<Double> originalWeights, List<Long> sampledTimestamps, List<T> sampledValues,
      double bucketWeight) {
    if (bucketWeight == 0) {
      sampledTimestamps.addAll(originalTimestamps);
      sampledValues.addAll(originalValues);
      return;
    }

    // find a bucket[lo, hi] and sample
    bucketWeight /= getSampleSizeInSingleBucket();
    int loIndex = 0;
    int hiIndex = getNextHiIndex(originalWeights, bucketWeight, -1);
    while (hiIndex != INVALID_INDEX) {
      sampleFromSingleBucket(originalTimestamps, originalValues, sampledTimestamps, sampledValues,
          loIndex, hiIndex);
      loIndex = hiIndex + 1;
      hiIndex = getNextHiIndex(originalWeights, bucketWeight, hiIndex);
    }
  }

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

  protected abstract int getSampleSizeInSingleBucket();

  protected abstract void sampleFromSingleBucket(List<Long> originalTimestamps,
      List<T> originalValues, List<Long> sampledTimestamps, List<T> sampledValues,
      final int loIndex, final int hiIndex);
}
