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

package org.apache.iotdb.db.sink;

import org.apache.iotdb.db.sink.alertmanager.AlertManagerConfiguration;
import org.apache.iotdb.db.sink.alertmanager.AlertManagerEvent;
import org.apache.iotdb.db.sink.alertmanager.AlertManagerHandler;
import org.junit.Test;

import java.util.HashMap;

public class AlertManagerTest {

    @Test
    public void alertmanagerTest() throws Exception {
        AlertManagerConfiguration alertManagerConfiguration =
                new AlertManagerConfiguration("http://127.0.0.1:9093/api/v1/alerts");
        AlertManagerHandler alertManagerHandler = new AlertManagerHandler();

        alertManagerHandler.open(alertManagerConfiguration);

        HashMap<String, String> labels = new HashMap<>();

        labels.put("alertname", "test1");
        labels.put("severity", "critical");
        labels.put("series", "root.ln.wt01.wf01.temperature");
        labels.put("value", String.valueOf(100.0));

        AlertManagerEvent alertManagerEvent = new AlertManagerEvent(labels);

        alertManagerHandler.onEvent(alertManagerEvent);

    }

    @Test
    public void alertmanagerTest2() throws Exception {
        AlertManagerConfiguration alertManagerConfiguration =
                new AlertManagerConfiguration("http://127.0.0.1:9093/api/v1/alerts");
        AlertManagerHandler alertManagerHandler = new AlertManagerHandler();

        alertManagerHandler.open(alertManagerConfiguration);

        HashMap<String, String> labels = new HashMap<>();

        labels.put("alertname", "test2");
        labels.put("severity", "critical");
        labels.put("series", "root.ln.wt01.wf01.temperature");
        labels.put("value", String.valueOf(100.0));

        HashMap<String, String> annotations = new HashMap<>();

        annotations.put("summary", "high temperature");
        annotations.put("description", "{{.series}}_avg is {{.value}}");

        AlertManagerEvent alertManagerEvent = new AlertManagerEvent(labels, annotations);

        alertManagerHandler.onEvent(alertManagerEvent);

    }
}
