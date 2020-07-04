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

package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.qp.logical.Operator.OperatorType;

public class HiFiQueryPlan extends RawDataQueryPlan {

  private String hiFiWeightOperatorName;
  private String hiFiSampleOperatorName;
  private int hiFiSampleSize;

  public HiFiQueryPlan() {
    super();
    setOperatorType(OperatorType.HIFI);
  }

  public String getHiFiWeightOperatorName() {
    return hiFiWeightOperatorName;
  }

  public void setHiFiWeightOperatorName(String hiFiWeightOperatorName) {
    this.hiFiWeightOperatorName = hiFiWeightOperatorName;
  }

  public String getHiFiSampleOperatorName() {
    return hiFiSampleOperatorName;
  }

  public void setHiFiSampleOperatorName(String hiFiSampleOperatorName) {
    this.hiFiSampleOperatorName = hiFiSampleOperatorName;
  }

  public int getHiFiSampleSize() {
    return hiFiSampleSize;
  }

  public void setHiFiSampleSize(int hiFiSampleSize) {
    this.hiFiSampleSize = hiFiSampleSize;
  }
}
