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
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class WeightOperator<T extends Number & Comparable<? super T>> {

  protected Double SPECIAL_POINT_WEIGHT = Double.MAX_VALUE;

  protected double weightSum = 0d;
  protected long count = 0L;

  public abstract Double operator(Long time0, Long time1, Long time2, T value0, T value1, T value2);

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

  public Double getCurrentAverageWeight() {
    return weightSum / count;
  }

  static public WeightOperator<?> getWeightOperator(String name, TSDataType type) {
    switch (name.toLowerCase()) {
      case SQLConstant.EUCLID_DISTANCE_OPERATOR:
        switch (type) {
          case INT32:
            return new EuclidDistanceOperator<Integer>();
          case INT64:
            return new EuclidDistanceOperator<Long>();
          case FLOAT:
            return new EuclidDistanceOperator<Float>();
          case DOUBLE:
            return new EuclidDistanceOperator<Double>();
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      case SQLConstant.MANHATTAN_DISTANCE_OPERATOR:
        switch (type) {
          case INT32:
            return new ManhattanDistanceOperator<Integer>();
          case INT64:
            return new ManhattanDistanceOperator<Long>();
          case FLOAT:
            return new ManhattanDistanceOperator<Float>();
          case DOUBLE:
            return new ManhattanDistanceOperator<Double>();
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      case SQLConstant.TRIANGULAR_AREA_OPERATOR:
        switch (type) {
          case INT32:
            return new TriangularAreaOperator<Integer>();
          case INT64:
            return new TriangularAreaOperator<Long>();
          case FLOAT:
            return new TriangularAreaOperator<Float>();
          case DOUBLE:
            return new TriangularAreaOperator<Double>();
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      case SQLConstant.COSINE_OPERATOR:
        switch (type) {
          case INT32:
            return new CosineOperator<Integer>();
          case INT64:
            return new CosineOperator<Long>();
          case FLOAT:
            return new CosineOperator<Float>();
          case DOUBLE:
            return new CosineOperator<Double>();
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      default:
        throw new UnsupportedOperationException("Weight operator \"" + name + "\" is unsupported.");
    }
  }
}
