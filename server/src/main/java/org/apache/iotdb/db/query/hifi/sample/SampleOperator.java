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

package org.apache.iotdb.db.query.hifi.sample;

import java.util.List;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

public interface SampleOperator<T extends Number> {

  /**
   * @param bucketWeight The weight of the bucket when only one point is sampled
   */
  void sample(List<Long> originalTimestamps, List<T> originalValuesList,
      List<Byte> originalBitmapList, List<Double> originalWeightsList, PublicBAOS timeBAOS,
      PublicBAOS valueBAOSList, PublicBAOS bitmapBAOSList, TSDataType type, double bucketWeight);

  static SampleOperator<?> getSampleOperator(String name, TSDataType type) {
    switch (name.toLowerCase()) {
      case "m4":
        switch (type) {
          case INT32:
            new M4Operator<Integer>();
          case INT64:
            new M4Operator<Long>();
          case FLOAT:
            new M4Operator<Float>();
          case DOUBLE:
            new M4Operator<Double>();
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", type));
        }
      default:
        throw new UnsupportedOperationException("Sample operator \"" + name + "\" is unsupported");
    }
  }
}
