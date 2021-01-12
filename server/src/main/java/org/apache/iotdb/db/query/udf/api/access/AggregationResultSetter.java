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

package org.apache.iotdb.db.query.udf.api.access;

import org.apache.iotdb.db.query.udf.api.UDAF;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

/**
 * Used to set the aggregation result in {@link UDAF#terminate(AggregationResultSetter)}.
 */
public interface AggregationResultSetter {

  /**
   * Sets the aggregation result to null.
   */
  void setNull();

  /**
   * Sets the aggregation result to an int value.
   * <p>
   * Before calling this method, you need to ensure that the UDAF output data type is set to {@code
   * TSDataType.INT32} by calling {@link UDFConfigurations#setOutputDataType(TSDataType)} in {@link
   * UDAF#beforeStart(UDFParameters, UDFConfigurations)}.
   *
   * @param value int value to set
   * @see TSDataType
   */
  void setInt(int value);

  /**
   * Sets the aggregation result to a long value.
   * <p>
   * Before calling this method, you need to ensure that the UDAF output data type is set to {@code
   * TSDataType.INT64} by calling {@link UDFConfigurations#setOutputDataType(TSDataType)} in {@link
   * UDAF#beforeStart(UDFParameters, UDFConfigurations)}.
   *
   * @param value long value to set
   * @see TSDataType
   */
  void setLong(long value);

  /**
   * Sets the aggregation result to a float value.
   * <p>
   * Before calling this method, you need to ensure that the UDAF output data type is set to {@code
   * TSDataType.FLOAT} by calling {@link UDFConfigurations#setOutputDataType(TSDataType)} in {@link
   * UDAF#beforeStart(UDFParameters, UDFConfigurations)}.
   *
   * @param value float value to set
   * @see TSDataType
   */
  void setFloat(float value);

  /**
   * Sets the aggregation result to a double value.
   * <p>
   * Before calling this method, you need to ensure that the UDAF output data type is set to {@code
   * TSDataType.DOUBLE} by calling {@link UDFConfigurations#setOutputDataType(TSDataType)} in {@link
   * UDAF#beforeStart(UDFParameters, UDFConfigurations)}.
   *
   * @param value double value to set
   * @see TSDataType
   */
  void setDouble(double value);

  /**
   * Sets the aggregation result to a boolean value.
   * <p>
   * Before calling this method, you need to ensure that the UDAF output data type is set to {@code
   * TSDataType.BOOLEAN} by calling {@link UDFConfigurations#setOutputDataType(TSDataType)} in
   * {@link UDAF#beforeStart(UDFParameters, UDFConfigurations)}.
   *
   * @param value boolean value to set
   * @see TSDataType
   */
  void setBoolean(boolean value);

  /**
   * Sets the aggregation result to a Binary value.
   * <p>
   * Before calling this method, you need to ensure that the UDAF output data type is set to {@code
   * TSDataType.TEXT} by calling {@link UDFConfigurations#setOutputDataType(TSDataType)} in {@link
   * UDAF#beforeStart(UDFParameters, UDFConfigurations)}.
   *
   * @param value Binary value to set
   * @see TSDataType
   */
  void setBinary(Binary value);

  /**
   * Sets the aggregation result to a String value.
   * <p>
   * Before calling this method, you need to ensure that the UDAF output data type is set to {@code
   * TSDataType.TEXT} by calling {@link UDFConfigurations#setOutputDataType(TSDataType)} in {@link
   * UDAF#beforeStart(UDFParameters, UDFConfigurations)}.
   *
   * @param value String value to set
   * @see TSDataType
   */
  void setString(String value);
}
