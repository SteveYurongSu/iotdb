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
  private final TriggerParameterConfigurations parameters;
  private boolean active;

  public Trigger(String path, String id, int enabledHooks,
      TriggerParameterConfigurations parameters, boolean isActive) {
    this.path = path;
    this.id = id;
    this.enabledHooks = enabledHooks;
    this.parameters = parameters;
    this.active = isActive;
    onConfig(parameters);
  }

  abstract public boolean isSynced();

  public void onConfig(TriggerParameterConfigurations parameters) {
  }

  public void beforeStart() {
  }

  public void afterStop() {
  }

  public boolean conditionBeforeInsertRecord(final long timestamp, final Object value) {
    return true;
  }

  public boolean conditionAfterInsertRecord(final long timestamp, final Object value) {
    return true;
  }

  public boolean conditionBeforeInsertTablet(final long[] timestamps, final Object[] values) {
    return true;
  }

  public boolean conditionAfterInsertTablet(final long[] timestamps, final Object[] values) {
    return true;
  }

  public boolean conditionBeforeDelete(final long timestamp) {
    return true;
  }

  public boolean conditionAfterDelete(final long timestamp) {
    return true;
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

  public final TriggerParameterConfigurations getParameters() {
    return parameters;
  }

  @Override
  public final String toString() {
    return "Trigger\n\tPath: " + getPath()
        + "\n\tID: " + getId()
        + "\n\tClass Name: " + getClass().getName()
        + "\n\tParameters: \n" + parameters.toString();
  }
}
