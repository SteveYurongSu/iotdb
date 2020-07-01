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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.tsfile.read.common.Field;

public class HiFiQueryPlan extends RawDataQueryPlan {

  private String hiFiWeightOperatorName;
  private String hiFiSampleOperatorName;
  private int hiFiSampleSize;
  private boolean aggregationPlanHasSetup;
  private final AggregationPlan aggregationPlan;
  private long[] counts;
  private double[] averageBucketSize;

  public HiFiQueryPlan() {
    super();
    setOperatorType(OperatorType.HIFI);
    aggregationPlanHasSetup = false;
    aggregationPlan = new AggregationPlan();
  }

  public AggregationPlan getAggregationPlan() {
    if (!aggregationPlanHasSetup) {
      setupInternalAggregationPlan();
      aggregationPlanHasSetup = true;
    }
    return aggregationPlan;
  }

  public void setCountsAndAverageBucketSize(SingleDataSet queryDataSet) throws IOException {
    counts = new long[paths.size()];
    averageBucketSize = new double[paths.size()];
    List<Field> fields = queryDataSet.next().getFields();
    for (int i = 0; i < paths.size(); ++i) {
      long count = fields.get(i).getLongV();
      counts[i] = count;
      averageBucketSize[i] = (double) count / hiFiSampleSize;
    }
  }

  public long[] getCounts() {
    return counts;
  }

  public double[] getAverageBucketSize() {
    return averageBucketSize;
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

  private void setupInternalAggregationPlan() {
    aggregationPlan.setOperatorType(OperatorType.AGGREGATION);
    aggregationPlan.setQuery(true);
    aggregationPlan.setAlignByTime(true);
    aggregationPlan.setPaths(getPaths());
    aggregationPlan.setDeduplicatedPaths(getDeduplicatedPaths());
    aggregationPlan.setDataTypes(getDataTypes());
    aggregationPlan.setDeduplicatedDataTypes(getDeduplicatedDataTypes());
    aggregationPlan.setExpression(getExpression());
    List<String> aggregations = new ArrayList<>(paths.size());
    for (int i = 0; i < paths.size(); ++i) {
      aggregations.add(SQLConstant.COUNT);
    }
    aggregationPlan.setAggregations(aggregations);
    aggregationPlan.setDeduplicatedAggregations(aggregations);
    aggregationPlan.setLevel(-1);
  }
}
