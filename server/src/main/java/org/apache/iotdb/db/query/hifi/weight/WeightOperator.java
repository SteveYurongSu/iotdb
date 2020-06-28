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
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public interface WeightOperator<T extends Number> {

  Double SPECIAL_POINT_WEIGHT = Double.MAX_VALUE;

  void calculate(List<Long> originalTimestamps, List<T> originalValues, List<Byte> originalBitmap,
      List<Double> originalWeights);

  Double operator(Long time0, Long time1, Long time2, T value0, T value1, T value2);

  Double getCurrentAverageWeight();

  static WeightOperator<?> getWeightOperator(String name, TSDataType type) {
    switch (name.toLowerCase()) {
      case "euclid":
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
      default:
        throw new UnsupportedOperationException("Weight operator \"" + name + "\" is unsupported");
    }
  }
}
