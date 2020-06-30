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

public class AverageOperator<T extends Number & Comparable<? super T>> extends SampleOperator<T> {

  public AverageOperator(TSDataType type) {
    super(type);
  }

  @Override
  protected int getSampleSizeInSingleBucket() {
    return 1;
  }

  @Override
  protected void sampleFromSingleBucket(List<Long> originalTimestamps, List<T> originalValues,
      List<Long> sampledTimestamps, List<T> sampledValues, final int loIndex, final int hiIndex) {
    long timeSum = 0;
    double valueSum = 0;
    for (int i = loIndex; i < hiIndex + 1; ++i) {
      timeSum += originalTimestamps.get(i);
      valueSum += originalValues.get(i).doubleValue();
    }
    int count = hiIndex - loIndex + 1;
    sampledTimestamps.add(timeSum / count);
    switch (type) {
      case INT32:
        sampledValues.add((T) Integer.valueOf((int) Math.round(valueSum / count)));
      case INT64:
        sampledValues.add((T) Long.valueOf(Math.round(valueSum / count)));
      case FLOAT:
        sampledValues.add((T) Float.valueOf((float) (valueSum / count)));
      case DOUBLE:
        sampledValues.add((T) Double.valueOf(valueSum / count));
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", type));
    }
  }
}
