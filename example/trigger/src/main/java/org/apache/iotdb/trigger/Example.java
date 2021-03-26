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

package org.apache.iotdb.trigger;

import org.apache.iotdb.db.engine.trigger.api.Trigger;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.sink.ts.TimeSeriesConfiguration;
import org.apache.iotdb.db.sink.ts.TimeSeriesEvent;
import org.apache.iotdb.db.sink.ts.TimeSeriesHandler;
import org.apache.iotdb.db.utils.windowing.configuration.SlidingTimeWindowConfiguration;
import org.apache.iotdb.db.utils.windowing.handler.SlidingTimeWindowEvaluationHandler;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class Example implements Trigger {

  private final TimeSeriesHandler timeSeriesHandler = new TimeSeriesHandler();

  private SlidingTimeWindowEvaluationHandler windowEvaluationHandler;

  @Override
  public void onStart() throws Exception {
    timeSeriesHandler.open(
        new TimeSeriesConfiguration(
            "root.sg1.d1", new String[] {"s1"}, new TSDataType[] {TSDataType.INT32}));

    windowEvaluationHandler =
        new SlidingTimeWindowEvaluationHandler(
            new SlidingTimeWindowConfiguration(TSDataType.DOUBLE, 1000, 1000),
            window -> {
              if (window.size() == 0) {
                return;
              }

              long time = window.getTime(0);
              double avg = 0;
              for (int i = 0; i < window.size(); ++i) {
                avg += window.getInt(i);
              }
              avg /= window.size();

              try {
                timeSeriesHandler.onEvent(new TimeSeriesEvent(time, avg));
              } catch (QueryProcessException
                  | StorageEngineException
                  | StorageGroupNotSetException e) {
                e.printStackTrace();
              }
            });
  }

  @Override
  public Integer fire(long timestamp, Integer value) {
    windowEvaluationHandler.collect(timestamp, value);
    return value;
  }
}
