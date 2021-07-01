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

package org.apache.iotdb.db.query.udf.builtin;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class MManagerHook implements UDTF {

  String p;

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    p = parameters.getString("p");
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(TSDataType.FLOAT);
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    Set<PartialPath> devices = IoTDB.metaManager.getDevices(new PartialPath(p));
    Map<PartialPath, MeasurementSchema> measurementSchemaMap = new HashMap<>();
    List<PartialPath> unmergedSeries = new ArrayList<>();
    for (PartialPath device : devices) {
      System.out.println("");
      System.out.println("++++ device: " + device);
      MNode deviceNode = IoTDB.metaManager.getNodeByPath(device);
      for (Entry<String, MNode> entry : deviceNode.getChildren().entrySet()) {
        PartialPath path = device.concatNode(entry.getKey());
        measurementSchemaMap.put(path, ((MeasurementMNode) entry.getValue()).getSchema());
        unmergedSeries.add(path);
        System.out.println("path: " + path.getFullPath());
      }
    }
    System.out.println("");
    System.out.println("");
  }
}

// select h(ACCtl_tiMnSwtDel_mp, 'p'='root.ivc_pems_0') from
// root.ivc_pdc_0.73d0764e1e14b3349977072bc0a59f15
// select h(ACCtl_tiMnSwtDel_mp, 'p'='root.ivc_iov_0') from
// root.ivc_pdc_0.73d0764e1e14b3349977072bc0a59f15
// select h(ACCtl_tiMnSwtDel_mp, 'p'='root.ivc_pdc_0') from
// root.ivc_pdc_0.73d0764e1e14b3349977072bc0a59f15
