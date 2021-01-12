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

package org.apache.iotdb.db.query.udf.api;

import org.apache.iotdb.db.query.udf.api.access.AggregationResultSetter;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;

/**
 * User-defined Aggregation Function (UDAF)
 * <p>
 * New UDAF classes need to inherit from this UDAF class.
 * <p>
 * A complete UDAF needs to override at least the following methods:
 * <ul>
 * <li>{@link UDAF#beforeStart(UDFParameters, UDFConfigurations)}
 * <li>{@link UDAF#iterate(Row)} or {@link UDAF#iterate(RowWindow)}
 * <li>{@link UDAF#terminate(AggregationResultSetter)}
 * </ul>
 * In the life cycle of a UDAF instance, the calling sequence of each method is as follows:
 * <p>
 * 1. {@link UDAF#validate(UDFParameterValidator)}
 * 2. {@link UDAF#beforeStart(UDFParameters, UDFConfigurations)}
 * 3. {@link UDAF#iterate(Row)} or {@link UDAF#iterate(RowWindow)}
 * 4. {@link UDAF#terminate(AggregationResultSetter)}
 * 5. {@link UDAF#beforeDestroy()}
 * <p>
 * The query engine will instantiate an independent UDAF instance for each udf query column, and
 * different UDAF instances will not affect each other.
 */
public interface UDAF extends UDF {

  /**
   * When the user specifies {@link RowByRowAccessStrategy} to access the original data in {@link
   * UDFConfigurations}, this method will be called to process the aggregation. In a single UDF
   * query, this method may be called multiple times.
   *
   * @param row original input data row (aligned by time)
   * @throws Exception the user can throw errors if necessary
   * @see RowByRowAccessStrategy
   */
  @SuppressWarnings("squid:S112")
  default void iterate(Row row) throws Exception {
  }

  /**
   * When the user specifies {@link SlidingSizeWindowAccessStrategy} or {@link
   * SlidingTimeWindowAccessStrategy} to access the original data in {@link UDFConfigurations}, this
   * method will be called to process the aggregation. In a single UDF query, this method may be
   * called multiple times.
   *
   * @param rowWindow original input data window (rows inside the window are aligned by time)
   * @throws Exception the user can throw errors if necessary
   * @see SlidingSizeWindowAccessStrategy
   * @see SlidingTimeWindowAccessStrategy
   */
  @SuppressWarnings("squid:S112")
  default void iterate(RowWindow rowWindow) throws Exception {
  }

  /**
   * This method is used to set the aggregation result.
   * <p>
   * This method will be called once after all {@link UDAF#iterate(Row)} calls or {@link
   * UDAF#iterate(RowWindow)} calls have been executed. In a single UDF query, this method will and
   * will only be called once.
   *
   * @param aggregationResultSetter used to set the aggregation result
   * @throws Exception the user can throw errors if necessary
   */
  @SuppressWarnings("squid:S112")
  default void terminate(AggregationResultSetter aggregationResultSetter) throws Exception {
  }
}
