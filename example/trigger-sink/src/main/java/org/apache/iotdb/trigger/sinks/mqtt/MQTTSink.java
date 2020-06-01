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

package org.apache.iotdb.trigger.sinks.mqtt;

import org.apache.iotdb.db.trigger.sink.TriggerSink;
import org.apache.iotdb.db.trigger.sink.TriggerSinkEvent;
import org.apache.iotdb.db.trigger.sink.TriggerSinkParameterConfigurations;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

public class MQTTSink extends TriggerSink {

  private MQTT mqtt;
  private BlockingConnection connection;
  private final String host;
  private final int port;
  private final String userName;
  private final String password;

  public MQTTSink(TriggerSinkParameterConfigurations configurations) {
    super(configurations);
    MQTTParameterConfiguration mqttParameterConfiguration = (MQTTParameterConfiguration) configurations;
    host = mqttParameterConfiguration.getHost();
    port = mqttParameterConfiguration.getPort();
    userName = mqttParameterConfiguration.getUserName();
    password = mqttParameterConfiguration.getPassword();
  }

  @Override
  public void open() {
    try {
      mqtt = new MQTT();
      mqtt.setHost(host, port);
      mqtt.setUserName(userName);
      mqtt.setPassword(password);
      connection = mqtt.blockingConnection();
      connection.connect();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    try {
      connection.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onEvent(TriggerSinkEvent event) {
    MQTTEvent mqttEvent = (MQTTEvent) event;
    try {
      connection.publish(mqttEvent.getTopic(), mqttEvent.getPayload().getBytes(), QoS.AT_LEAST_ONCE,
          false);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
