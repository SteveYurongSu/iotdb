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

package org.apache.iotdb.db.query.udf.builtin;

import java.io.IOException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class UDTFHiFiCos implements UDTF {

  private static final String ATTRIBUTE_THRESHOLD = "threshold";

  private double threshold;
  private TSDataType dataType;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1) // 确保输入序列只有一个
        .validateInputSeriesDataType(0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
            TSDataType.DOUBLE) // 确保输入序列类型为数值类型
        .validateRequiredAttribute(ATTRIBUTE_THRESHOLD) // 要求用户提供参数 threshold
        .validate(threshold -> -1 <= (double) threshold && (double) threshold <= 1,
            "threshold has to be greater than or equal to -1 and less than or equal to 1.",
            validator.getParameters().getDouble(ATTRIBUTE_THRESHOLD)); // 限定参数 threshold 的范围 [-1, 1]
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException {
    configurations
        .setOutputDataType(parameters.getDataType(0)) // 输出序列类型与输入序列相同
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(3, 1)); // 按照3行数据一个窗口的方式迭代

    threshold = parameters.getDouble(ATTRIBUTE_THRESHOLD); // 从用户SQL中获取参数 threshold
    dataType = parameters.getDataType(0);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    if (rowWindow.windowSize() < 3) { // 序列最后的一些窗口，不输出任何值
      return;
    }

    switch (dataType) {
      case INT32:
        transformInt(rowWindow, collector);
        break;
      case INT64:
        transformLong(rowWindow, collector);
        break;
      case FLOAT:
        transformFloat(rowWindow, collector);
        break;
      case DOUBLE:
        transformDouble(rowWindow, collector);
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void transformDouble(RowWindow rowWindow, PointCollector collector) throws IOException {
    long lastTime = rowWindow.getRow(0).getTime();
    long currentTime = rowWindow.getRow(1).getTime();
    long nextTime = rowWindow.getRow(2).getTime();

    double lastValue = rowWindow.getRow(0).getDouble(0);
    double currentValue = rowWindow.getRow(1).getDouble(0);
    double nextValue = rowWindow.getRow(2).getDouble(0);

    long x1 = currentTime - lastTime;
    long x2 = nextTime - currentTime;
    double y1 = currentValue - lastValue;
    double y2 = nextValue - currentValue;
    double cos =
        (x1 * x2 + y1 * y2) / (Math.sqrt(x1 * x1 + y1 * y1) * Math.sqrt(x2 * x2 + y2 * y2));

    if (cos <= threshold) {
      collector.putDouble(currentTime, currentValue);
    }
  }

  private void transformFloat(RowWindow rowWindow, PointCollector collector) throws IOException {
    long lastTime = rowWindow.getRow(0).getTime();
    long currentTime = rowWindow.getRow(1).getTime();
    long nextTime = rowWindow.getRow(2).getTime();

    float lastValue = rowWindow.getRow(0).getFloat(0);
    float currentValue = rowWindow.getRow(1).getFloat(0);
    float nextValue = rowWindow.getRow(2).getFloat(0);

    long x1 = currentTime - lastTime;
    long x2 = nextTime - currentTime;
    float y1 = currentValue - lastValue;
    float y2 = nextValue - currentValue;
    double cos =
        (x1 * x2 + y1 * y2) / (Math.sqrt(x1 * x1 + y1 * y1) * Math.sqrt(x2 * x2 + y2 * y2));

    if (cos <= threshold) {
      collector.putFloat(currentTime, currentValue);
    }
  }

  private void transformLong(RowWindow rowWindow, PointCollector collector) throws IOException {
    long lastTime = rowWindow.getRow(0).getTime();
    long currentTime = rowWindow.getRow(1).getTime();
    long nextTime = rowWindow.getRow(2).getTime();

    long lastValue = rowWindow.getRow(0).getLong(0);
    long currentValue = rowWindow.getRow(1).getLong(0);
    long nextValue = rowWindow.getRow(2).getLong(0);

    long x1 = currentTime - lastTime;
    long x2 = nextTime - currentTime;
    long y1 = currentValue - lastValue;
    long y2 = nextValue - currentValue;
    double cos = (x1 * x2 + y1 * y2)
        / (Math.sqrt((double) x1 * x1 + y1 * y1) * Math.sqrt((double) x2 * x2 + y2 * y2));

    if (cos <= threshold) {
      collector.putLong(currentTime, currentValue);
    }
  }

  private void transformInt(RowWindow rowWindow, PointCollector collector) throws IOException {
    long lastTime = rowWindow.getRow(0).getTime();
    long currentTime = rowWindow.getRow(1).getTime();
    long nextTime = rowWindow.getRow(2).getTime();

    int lastValue = rowWindow.getRow(0).getInt(0);
    int currentValue = rowWindow.getRow(1).getInt(0);
    int nextValue = rowWindow.getRow(2).getInt(0);

    long x1 = currentTime - lastTime;
    long x2 = nextTime - currentTime;
    int y1 = currentValue - lastValue;
    int y2 = nextValue - currentValue;
    double cos = (x1 * x2 + y1 * y2)
        / (Math.sqrt((double) x1 * x1 + y1 * y1) * Math.sqrt((double) x2 * x2 + y2 * y2));

    if (cos <= threshold) {
      collector.putInt(currentTime, currentValue);
    }
  }
}
