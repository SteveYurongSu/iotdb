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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;

public class ImportTool {

  private static final double ERROR_BOUND = 0.10;

  private static Session sourceSession;
  private static Session targetSession;

  public static void main(String[] args) throws Exception {
    sourceSession = new Session("127.0.0.1", 6667, "root", "root");
    sourceSession.open(false);
    targetSession = new Session("127.0.0.1", 6668, "root", "root");
    targetSession.open(false);

    importAll("root.group_69");

    targetSession.close();
    sourceSession.close();
  }

  private static void importAll(String sg)
      throws StatementExecutionException, IoTDBConnectionException, IOException, IllegalPathException {
    List<String> devices = getAllDevices(sg);
    int count = 0;
    for (String device : devices) {
      ++count;
      System.out.println("===============================================");
      System.out.println(count + " / " + devices.size());
      System.out.println("===============================================");
      long startTime = System.currentTimeMillis();
      Statistics statistics = new Statistics(device);
      doImport(device, statistics);
      statistics.report();
      long endTime = System.currentTimeMillis();
      System.out.println(
          "Time cost (" + count + " / " + devices.size() + "): " + (endTime - startTime) + "ms");
    }
  }

  private static void doImport(String device, Statistics statistics)
      throws StatementExecutionException, IoTDBConnectionException, IllegalPathException {
    SessionDataSet sessionDataSet = sourceSession.executeQueryStatement("select * from " + device);
    List<String> columnNames = sessionDataSet.getColumnNames();
    List<TSDataType> columnTypes = sessionDataSet.getColumnTypes();
    System.out.println("Creating timeseries...");
    createTimeSeries(columnTypes, columnNames);
    System.out.println("Creating timeseries ... done!");
    int count = 0;
    while (sessionDataSet.hasNext()) {
      if (++count % 1000 == 0) {
        System.out.println("Importing ... " + count);
      }
      RowRecord rowRecord = sessionDataSet.next();
      importRecord(device, rowRecord, columnNames, columnTypes, statistics);
    }
  }

  private static void importRecord(String deviceId, RowRecord rowRecord,
      List<String> columnNames, List<TSDataType> columnTypes, Statistics statistics)
      throws StatementExecutionException, IoTDBConnectionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<Object> values = new ArrayList<>();

    List<Field> fields = rowRecord.getFields();
    for (int i = 0; i < fields.size(); ++i) {
      Field field = fields.get(i);
      if (field == null || field.isNull()) {
        continue;
      }
      Object o = field.getObjectValue(columnTypes.get(i));
      if (o instanceof Binary) {
        o = ((Binary) o).getStringValue();
      }
      if (o == null) {
        continue;
      }
      values.add(o);
      measurements.add(columnNames.get(i));
      types.add(columnTypes.get(i));
      statistics.update(columnTypes.get(i));
    }

    try {
      targetSession.insertRecord(deviceId, rowRecord.getTimestamp(), measurements, types, values);
    } catch (NullPointerException e) {
      e.printStackTrace();
      System.out.println(fields);
      System.out.println(values.contains(null));
      throw e;
    }
  }

  private static List<String> getAllDevices(String sg)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet sessionDataSet = sourceSession.executeQueryStatement("show devices " + sg);
    List<String> devices = new ArrayList<>();
    while (sessionDataSet.hasNext()) {
      devices.add(sessionDataSet.next().getFields().get(0).getStringValue());
      System.out.println(devices.get(devices.size() - 1));
    }
    return devices;
  }

  private static void createTimeSeries(List<TSDataType> columnTypes, List<String> columnNames)
      throws StatementExecutionException, IllegalPathException, IoTDBConnectionException {
    for (int i = 1; i < columnNames.size(); ++i) {
      if (i % 100 == 0) {
        System.out.println((i - 1) + " / " + (columnNames.size() - 1));
      }
      createTimeSeries(columnTypes.get(i), columnNames.get(i));
    }
  }

  private static void createTimeSeries(TSDataType dataType, String seriesPath)
      throws StatementExecutionException, IoTDBConnectionException, IllegalPathException {
    PartialPath path = new PartialPath(seriesPath);
    String device = path.getDevice();
    String sensor = path.getMeasurement();
    if (!targetSession.checkTimeseriesExists(seriesPath)) {
      Map<String, String> props = new HashMap<>();
      props.put("loss", "sdt");
      props.put("compMin", "2");
      props.put("compMax", "5000");
      switch (dataType) {
        case BOOLEAN:
          targetSession
              .createTimeseries(seriesPath, dataType, TSEncoding.RLE, CompressionType.SNAPPY, null,
                  null, null, null);
          break;
        case TEXT:
          targetSession
              .createTimeseries(seriesPath, dataType, TSEncoding.PLAIN, CompressionType.SNAPPY,
                  null, null, null, null);
          break;
        case INT32:
        case INT64:
          targetSession
              .createTimeseries(seriesPath, dataType, TSEncoding.GORILLA, CompressionType.SNAPPY,
                  null, null, null, null);
          break;
        case FLOAT:
        case DOUBLE:
          props.put("compDev",
              String.valueOf(ERROR_BOUND * getMaxMinDelta(dataType, device, sensor)));
          targetSession.createTimeseries(seriesPath, dataType, TSEncoding.GORILLA,
              CompressionType.SNAPPY, props, null, null, null);
          break;
        default:
          System.out.println("error occurred in createTimeSeries.");
      }
    }
  }

  private static double getMaxMinDelta(TSDataType dataType, String device, String sensor)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet sessionDataSet = sourceSession.executeQueryStatement(
        "select max_value(" + sensor + "), min_value(" + sensor + ") from " + device);
    if (sessionDataSet.hasNext()) {
      RowRecord rowRecord = sessionDataSet.next();
      if (rowRecord.getFields().get(0) == null || rowRecord.getFields().get(0).isNull()
          || rowRecord.getFields().get(1) == null || rowRecord.getFields().get(1).isNull()) {
        return 0;
      }
      switch (dataType) {
        case INT32:
          return (double) rowRecord.getFields().get(0).getIntV() - rowRecord.getFields().get(1)
              .getIntV();
        case INT64:
          return (double) rowRecord.getFields().get(0).getLongV() - rowRecord.getFields().get(1)
              .getLongV();
        case FLOAT:
          return (double) rowRecord.getFields().get(0).getFloatV() - rowRecord.getFields().get(1)
              .getFloatV();
        case DOUBLE:
          return rowRecord.getFields().get(0).getDoubleV() - rowRecord.getFields().get(1)
              .getDoubleV();
        default:
          break;
      }
    }
    throw new RuntimeException("Unsupported data type.");
  }

  private static class Statistics {

    private final int[] statistics;
    private final String device;

    public Statistics(String device) {
      this.statistics = new int[TSDataType.values().length];
      this.device = device;
    }

    void update(TSDataType dataType) {
      statistics[dataType.ordinal()]++;
    }

    void report() throws IOException {
      Appendable printWriter = new PrintWriter(new FileOutputStream("result.csv", true));
      CSVPrinter csvPrinter = CSVFormat.EXCEL.print(printWriter);
      csvPrinter.printRecord(device,
          statistics[TSDataType.INT32.ordinal()],
          statistics[TSDataType.INT64.ordinal()],
          statistics[TSDataType.FLOAT.ordinal()],
          statistics[TSDataType.DOUBLE.ordinal()],
          statistics[TSDataType.BOOLEAN.ordinal()],
          statistics[TSDataType.TEXT.ordinal()]
      );
      csvPrinter.flush();
      csvPrinter.close();
      System.out.printf("##### %s: %d, %d, %d, %d, %d, %d\n", device,
          statistics[TSDataType.INT32.ordinal()],
          statistics[TSDataType.INT64.ordinal()],
          statistics[TSDataType.FLOAT.ordinal()],
          statistics[TSDataType.DOUBLE.ordinal()],
          statistics[TSDataType.BOOLEAN.ordinal()],
          statistics[TSDataType.TEXT.ordinal()]);
    }
  }
}
