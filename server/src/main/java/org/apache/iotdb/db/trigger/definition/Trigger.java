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

public abstract class Trigger {

  private final String path;
  private final String id;
  private final int enabledHooks;
  private final TriggerParameterConfiguration[] parameters;
  private boolean active;

  public Trigger(String path, String id, int enabledHooks,
      TriggerParameterConfiguration[] parameters, boolean isActive) {
    this.path = path;
    this.id = id;
    this.enabledHooks = enabledHooks;
    this.parameters = parameters;
    this.active = isActive;
    onConfig(parameters);
  }

  abstract public boolean isSynced();

  public void onConfig(TriggerParameterConfiguration[] parameters) {
  }

  public void beforeStart() {
  }

  public void afterStop() {
  }

  public final void markAsActive() {
    active = true;
  }

  public final void markAsInactive() {
    active = false;
  }

  public final boolean isActive() {
    return active;
  }

  public final String getPath() {
    return path;
  }

  public final String getId() {
    return id;
  }

  public final int getEnabledHooks() {
    return enabledHooks;
  }

  public final TriggerParameterConfiguration[] getParameters() {
    return parameters;
  }

  @Override
  public final String toString() {
    StringBuilder stringBuilder = new StringBuilder("Trigger\n\tPath: ").append(getPath())
        .append("\n\tID: ").append(getId())
        .append("\n\tClass Name: ").append(getClass().getName())
        .append("\n\tParameters: ");
    for (TriggerParameterConfiguration parameterConfiguration : parameters) {
      stringBuilder.append("\n\t\t").append(parameterConfiguration.toString());
    }
    stringBuilder.append('\n');
    return stringBuilder.toString();
  }
}
