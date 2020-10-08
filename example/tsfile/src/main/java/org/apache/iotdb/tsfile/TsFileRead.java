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
package org.apache.iotdb.tsfile;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;

/**
 * The class is to show how to read TsFile file named "test.tsfile". The TsFile file "test.tsfile"
 * is generated from class TsFileWriteWithTSRecord or TsFileWriteWithTablet. Run
 * TsFileWriteWithTSRecord or TsFileWriteWithTablet to generate the test.tsfile first
 */
public class TsFileRead {

  private static final String DEVICE1 = "root.sg.d";

  private static long timeCount = 0;

  private static void queryAndPrint(ArrayList<Path> paths, ReadOnlyTsFile readTsFile,
      IExpression statement)
      throws IOException {
    long startTime = System.currentTimeMillis();
    QueryExpression queryExpression = QueryExpression.create(paths, statement);
    readTsFile.query(queryExpression);
    long endTime = System.currentTimeMillis();
    timeCount += endTime - startTime;
//    while (queryDataSet.hasNext()) {
//      System.out.println(queryDataSet.next());
//    }
//    System.out.println("------------");
  }

  public static void main(String[] args) throws IOException {

    for (int i = 0; i < 1000000; ++i) {
      testOnce();
    }

    System.out.println(timeCount);
  }

  public static void testOnce() throws IOException {
    // file path
    String path = "/Users/steve/Desktop/10.ts";

    // create reader and get the readTsFile interface
    try (TsFileSequenceReader reader = new TsFileSequenceReader(path);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader)) {

      // use these paths(all measurements) for all the queries
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(DEVICE1, "s"));

      // no filter, should select 1 2 3 4 6 7 8
      queryAndPrint(paths, readTsFile, null);
    }
  }
}
