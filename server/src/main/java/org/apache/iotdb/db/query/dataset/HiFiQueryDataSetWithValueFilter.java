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

package org.apache.iotdb.db.query.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.iotdb.db.qp.physical.crud.HiFiQueryPlan;
import org.apache.iotdb.db.query.hifi.sample.SampleOperator;
import org.apache.iotdb.db.query.hifi.weight.WeightOperator;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public class HiFiQueryDataSetWithValueFilter extends QueryDataSet {

  private final HiFiQueryPlan queryPlan;

  private final TimeGenerator timeGenerator;
  private final List<IReaderByTimestamp> seriesReaderByTimestampList;
  private final List<Boolean> cached;

  private final WeightOperator<?>[] weightOperators;
  private final SampleOperator<?>[] sampleOperators;
  private final double[] bucketWeights;

  private final List<Long>[] originalTimestampsList;
  private final List<Number>[] originalValuesList;
  private final List<Double>[] originalWeightsList;

  private final List<Long>[] sampledTimestampsList;
  private final List<Number>[] sampledValuesList;

  private final int[] readyToConsumeIndexList;
  private final TreeSet<Long> timeHeap;

  public HiFiQueryDataSetWithValueFilter(HiFiQueryPlan queryPlan, TimeGenerator timeGenerator,
      List<IReaderByTimestamp> readers, List<Boolean> cached) throws IOException {
    super(queryPlan.getDeduplicatedPaths(), queryPlan.getDeduplicatedDataTypes());
    this.queryPlan = queryPlan;
    this.timeGenerator = timeGenerator;
    this.seriesReaderByTimestampList = readers;
    this.cached = cached;
    int seriesNum = readers.size();
    weightOperators = new WeightOperator[seriesNum];
    sampleOperators = new SampleOperator[seriesNum];
    bucketWeights = new double[seriesNum];
    originalTimestampsList = new List[seriesNum];
    originalValuesList = new List[seriesNum];
    originalWeightsList = new List[seriesNum];
    sampledTimestampsList = new List[seriesNum];
    sampledValuesList = new List[seriesNum];
    for (int i = 0; i < seriesNum; ++i) {
      weightOperators[i] = WeightOperator.getWeightOperator(queryPlan.getHiFiWeightOperatorName(),
          getDataTypes().get(i));
      sampleOperators[i] = SampleOperator.getSampleOperator(queryPlan.getHiFiSampleOperatorName(),
          getDataTypes().get(i));
      originalTimestampsList[i] = new ArrayList<>();
      originalValuesList[i] = new ArrayList<>();
      originalWeightsList[i] = new ArrayList<>();
      sampledTimestampsList[i] = new ArrayList<>();
      sampledValuesList[i] = new ArrayList<>();
    }
    readyToConsumeIndexList = new int[seriesNum];
    fetch();
    calculatePointWeightsAndBucketWeights();
    hiFiSample();
    timeHeap = new TreeSet<>();
    for (List<Long> sampledTimestamps : sampledTimestampsList) {
      timeHeap.addAll(sampledTimestamps);
    }
  }

  @Override
  protected boolean hasNextWithoutConstraint() {
    return !timeHeap.isEmpty();
  }

  @Override
  protected RowRecord nextWithoutConstraint() {
    long minTime = timeHeap.pollFirst();
    RowRecord record = new RowRecord(minTime);

    for (int i = 0; i < seriesReaderByTimestampList.size(); ++i) {
      int currentIndex = readyToConsumeIndexList[i];
      if (sampledTimestampsList[i].size() <= currentIndex
          || sampledTimestampsList[i].get(currentIndex) != minTime) {
        record.addField(null);
      } else {
        record.addField(sampledValuesList[i].get(currentIndex), getDataTypes().get(i));
        // update index
        ++readyToConsumeIndexList[i];
      }
    }

    return record;
  }

  private void fetch() throws IOException {
    while (timeGenerator.hasNext()) {
      long timestamp = timeGenerator.next();
      for (int i = 0; i < seriesReaderByTimestampList.size(); ++i) {
        Object value;
        if (cached.get(i)) {
          // get value from readers in time generator
          value = timeGenerator.getValue(paths.get(i), timestamp);
        } else {
          // get value from series reader without filter
          value = seriesReaderByTimestampList.get(i).getValueInTimestamp(timestamp);
        }
        if (value != null) {
          originalTimestampsList[i].add(timestamp);
          TSDataType type = getDataTypes().get(i);
          switch (type) {
            case INT32:
              originalValuesList[i].add((Integer) value);
              break;
            case INT64:
              originalValuesList[i].add((Long) value);
              break;
            case FLOAT:
              originalValuesList[i].add((Float) value);
              break;
            case DOUBLE:
              originalValuesList[i].add((Double) value);
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", type));
          }
        }
      }
    }
  }

  private void calculatePointWeightsAndBucketWeights() {
    for (int i = 0; i < seriesReaderByTimestampList.size(); ++i) {
      weightOperators[i].calculate(originalTimestampsList[i], (List) originalValuesList[i],
          originalWeightsList[i]);
      double bucketSize = queryPlan.getAverageBucketSize()[i];
      bucketWeights[i] =
          bucketSize <= 1 ? 0 : bucketSize * weightOperators[i].getCurrentAverageWeight();
    }
  }

  private void hiFiSample() {
    for (int i = 0; i < seriesReaderByTimestampList.size(); ++i) {
      sampleOperators[i]
          .sample(originalTimestampsList[i], (List) originalValuesList[i], originalWeightsList[i],
              sampledTimestampsList[i], (List) sampledValuesList[i], bucketWeights[i]);
    }
  }
}
