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

import org.apache.iotdb.db.query.udf.api.customizer.config.UDFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;

public interface UDF {

  /**
   * This method is mainly used to validate {@link UDFParameters} and it is executed before {@link
   * UDF#beforeStart(UDFParameters, UDFConfigurations)} is called.
   *
   * @param validator the validator used to validate {@link UDFParameters}
   * @throws Exception if any parameter is not valid
   */
  @SuppressWarnings("squid:S112")
  default void validate(UDFParameterValidator validator) throws Exception {
  }

  /**
   * This method is mainly used to customize UDF. In this method, the user can do the following
   * things:
   * <ul>
   * <li> Use UDFParameters to get the time series paths and parse key-value pair attributes entered
   * by the user.
   * <li> Set the strategy to access the original data and set the output data type in
   * UDFConfigurations.
   * <li> Create resources, such as establishing external connections, opening files, etc.
   * </ul>
   * <p>
   * This method is called after the UDF is instantiated and before the beginning of the
   * transformation or iteration process.
   *
   * @param parameters     used to parse the input parameters entered by the user
   * @param configurations used to set the required properties in the UDF
   * @throws Exception the user can throw errors if necessary
   */
  @SuppressWarnings("squid:S112")
  void beforeStart(UDFParameters parameters, UDFConfigurations configurations) throws Exception;

  /**
   * This method is mainly used to release the resources used in the UDF.
   */
  default void beforeDestroy() {
  }
}
