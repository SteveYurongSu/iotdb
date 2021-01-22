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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

public class ImportTool {

  private static Session sourceSession;
  private static Session session9;
  private static Session session69;

  public static void main(String[] args) throws Exception {
    sourceSession = new Session("127.0.0.1", 6667, "root", "root");
    sourceSession.open(false);
    session9 = new Session("127.0.0.1", 6668, "root", "root");
    session9.open(false);
    session69 = new Session("127.0.0.1", 6669, "root", "root");
    session69.open(false);

    query9();
    query69();

    session69.close();
    session9.close();
    sourceSession.close();
  }

  private static void query9() throws IoTDBConnectionException, StatementExecutionException {
    final String sql = "select last_value(ZT11759) from root.group_9.1712 GROUP BY([2020-05-09T20:44:46.287+08:00, 2020-05-19T18:48:06.116+08:00), 5m) FILL (int64[PREVIOUS, 30m])";

    System.out.println(sql);

    long t1 = System.currentTimeMillis();
    for (int i = 0; i < 100; ++i) {
      SessionDataSet dataSet = sourceSession.executeQueryStatement(sql);
      while (dataSet.hasNext()) {
        dataSet.next();
      }
      dataSet.closeOperationHandle();
    }
    System.out.println("source: " + (System.currentTimeMillis() - t1));

    t1 = System.currentTimeMillis();
    for (int i = 0; i < 100; ++i) {
      SessionDataSet dataSet = session9.executeQueryStatement(sql);
      while (dataSet.hasNext()) {
        dataSet.next();
      }
      dataSet.closeOperationHandle();
    }
    System.out.println("target: " + (System.currentTimeMillis() - t1));
  }

  private static void query69() throws IoTDBConnectionException, StatementExecutionException {
    final String sql = "select last_value(ZT11709) from root.group_69.1701 GROUP BY([2020-06-09T23:51:35.267+08:00, 2020-06-17T23:17:14.887+08:00), 5m) FILL (double[PREVIOUS, 30m])";

    System.out.println(sql);

    long t1 = System.currentTimeMillis();
    for (int i = 0; i < 100; ++i) {
      SessionDataSet dataSet = sourceSession.executeQueryStatement(sql);
      while (dataSet.hasNext()) {
        dataSet.next();
      }
      dataSet.closeOperationHandle();
    }
    System.out.println("source: " + (System.currentTimeMillis() - t1));

    t1 = System.currentTimeMillis();
    for (int i = 0; i < 100; ++i) {
      SessionDataSet dataSet = session69.executeQueryStatement(sql);
      while (dataSet.hasNext()) {
        dataSet.next();
      }
      dataSet.closeOperationHandle();
    }
    System.out.println("target: " + (System.currentTimeMillis() - t1));
  }
}
