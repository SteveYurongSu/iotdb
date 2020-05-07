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

package org.apache.iotdb.db.trigger.definition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

public class TriggerParameterConfigurations {

  private final HashMap<String, String> configurations;

  public TriggerParameterConfigurations() {
    configurations = new HashMap<>();
  }

  public void put(String name, String value) {
    configurations.put(name, value);
  }

  public String getStringValue(String name) {
    return configurations.get(name);
  }

  public Integer getIntegerValue(String name) {
    String string;
    return (string = getStringValue(name)) == null ? null : Integer.parseInt(string);
  }

  public Long getLongValue(String name) {
    String string;
    return (string = getStringValue(name)) == null ? null : Long.parseLong(string);
  }

  public Boolean getBooleanValue(String name) {
    String string;
    return (string = getStringValue(name)) == null ? null : Boolean.parseBoolean(string);
  }

  public Float getFloatValue(String name) {
    String string;
    return (string = getStringValue(name)) == null ? null : Float.parseFloat(string);
  }

  public Double getDoubleValue(String name) {
    String string;
    return (string = getStringValue(name)) == null ? null : Double.parseDouble(string);
  }

  public Set<String> getConfigurationNameSet() {
    return configurations.keySet();
  }

  public Collection<String> getConfigurationStringValues() {
    return configurations.values();
  }

  public Set<Entry<String, String>> getConfigurationEntrySet() {
    return configurations.entrySet();
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    for (Entry<String, String> configuration : configurations.entrySet()) {
      stringBuilder.append("\t\tTriggerParameterConfiguration<").append(configuration.getKey())
          .append(", ").append(configuration.getValue()).append(">\n");
    }
    return stringBuilder.toString();
  }
}
