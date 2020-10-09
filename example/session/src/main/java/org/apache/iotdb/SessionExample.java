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
package org.apache.iotdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class SessionExample {

  private static Session session;

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    try {
      session.setStorageGroup("root.sg1");
    } catch (StatementExecutionException e) {
      if (e.getStatusCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()) {
        throw e;
      }
    }

//    createTimeseries();
//    insertRecord();
    query();
    session.close();
  }

  private static void createTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {

    if (!session.checkTimeseriesExists("root.sg1.d1.s1")) {
      session.createTimeseries("root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE,
          CompressionType.SNAPPY);
    }
  }

  private static void insertRecord() throws IoTDBConnectionException, StatementExecutionException {
//    String deviceId = "root.sg1.d1";
//    List<String> measurements = new ArrayList<>();
//    List<TSDataType> types = new ArrayList<>();
//    measurements.add("s1");
//    types.add(TSDataType.INT64);
//
//    for (long i = 0; i < 10; ++i) {
//      for (long j = i * 100_0000; j < (i + 1) * 100_0000; ++j) {
//        List<Object> values = new ArrayList<>();
//        values.add(j);
//        session.insertRecord(deviceId, j, measurements, types, values);
//      }
//      session.executeNonQueryStatement("flush");
//      System.out.println(i);
//    }

    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));

    Tablet tablet1 = new Tablet("root.sg1.d1", schemaList, 100);

    Map<String, Tablet> tabletMap = new HashMap<>();
    tabletMap.put("root.sg1.d1", tablet1);

    // Method 1 to add tablet data
    long timestamp = 0;

    for (int k = 0; k < 10; ++k) {
      for (int j = 0; j < 100; ++j) {
        for (long row = 0; row < 10000; row++) {
          int row1 = tablet1.rowSize++;
          tablet1.addTimestamp(row1, timestamp);
          for (int i = 0; i < 1; i++) {
            long value = new Random().nextLong();
            tablet1.addValue(schemaList.get(i).getMeasurementId(), row1, value);
          }
          if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
            session.insertTablets(tabletMap, true);
            tablet1.reset();
          }
          timestamp++;
        }
        if (tablet1.rowSize != 0) {
          session.insertTablets(tabletMap, true);
          tablet1.reset();
        }
      }
      session.executeNonQueryStatement("flush");
      System.out.println(k);
    }
  }

  private static void query() throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet;
    dataSet = session.executeQueryStatement("select * from root.sg1.d1");
    System.out.println(dataSet.getColumnNames());

    long start = System.currentTimeMillis();
    while (dataSet.hasNext()) {
      dataSet.next();
    }
    System.out.println(System.currentTimeMillis() - start);
    dataSet.closeOperationHandle();
  }
}