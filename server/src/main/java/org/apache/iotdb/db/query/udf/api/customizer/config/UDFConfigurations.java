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

package org.apache.iotdb.db.query.udf.api.customizer.config;

import java.time.ZoneId;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.UDAF;
import org.apache.iotdb.db.query.udf.api.UDF;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.AggregationResultSetter;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.access.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.AccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * Used in {@link UDF#beforeStart(UDFParameters, UDFConfigurations)}.
 * <p>
 * Supports calling methods in a chain.
 * <p>
 * Sample code:
 * <pre>{@code
 * @Override
 * public void beforeStart(UDFParameters parameters, UDFConfigurations configurations) {
 *   configurations
 *       .setOutputDataType(TSDataType.INT64)
 *       .setAccessStrategy(new RowByRowAccessStrategy());
 * }</pre>
 */
public class UDFConfigurations {

  private final ZoneId zoneId;

  public UDFConfigurations(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  private TSDataType outputDataType;

  /**
   * Used to specify the output data type of the UDF.
   * <p>
   * For UDTF: The data type you set here determines the type of data that the PointCollector in
   * {@link UDTF#transform(Row, PointCollector)}, {@link UDTF#transform(RowWindow, PointCollector)}
   * or {@link UDTF#terminate(PointCollector)} can receive.
   * <p>
   * For UDAF: The data type you set here determines the type of data that the
   * AggregationResultSetter in {@link UDAF#terminate(AggregationResultSetter)} can receive.
   *
   * @param outputDataType the output data type of the UDF
   * @return this
   * @see PointCollector
   * @see AggregationResultSetter
   */
  public UDFConfigurations setOutputDataType(TSDataType outputDataType) {
    this.outputDataType = outputDataType;
    return this;
  }

  private AccessStrategy accessStrategy;

  public AccessStrategy getAccessStrategy() {
    return accessStrategy;
  }

  /**
   * Used to specify the strategy for accessing raw query data in UDF.
   *
   * @param accessStrategy the specified access strategy. it should be an instance of {@link
   *                       AccessStrategy}.
   * @return this
   * @see RowByRowAccessStrategy
   * @see SlidingTimeWindowAccessStrategy
   * @see SlidingSizeWindowAccessStrategy
   */
  public UDFConfigurations setAccessStrategy(AccessStrategy accessStrategy) {
    this.accessStrategy = accessStrategy;
    if (accessStrategy instanceof SlidingTimeWindowAccessStrategy
        && ((SlidingTimeWindowAccessStrategy) accessStrategy).getZoneId() == null) {
      ((SlidingTimeWindowAccessStrategy) accessStrategy).setZoneId(zoneId);
    }
    return this;
  }

  public void check() throws QueryProcessException {
    if (outputDataType == null) {
      throw new QueryProcessException("UDF outputDataType is not set.");
    }
    if (accessStrategy == null) {
      throw new QueryProcessException("Access strategy is not set.");
    }
    accessStrategy.check();
  }

  public TSDataType getOutputDataType() {
    return outputDataType;
  }
}
