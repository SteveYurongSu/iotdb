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

package org.apache.iotdb.db.engine.trigger.service;

import org.apache.iotdb.db.engine.trigger.executor.TriggerEvent;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.CreateTriggerPlan;

import java.util.Map;

public class TriggerRegistrationInformation {

  private final String triggerName;
  private final TriggerEvent event;
  private final PartialPath fullPath;
  private final String className;
  private final Map<String, String> attributes;

  private Class<?> triggerClass;

  private volatile boolean isStopped;

  public TriggerRegistrationInformation(
      String triggerName,
      TriggerEvent event,
      PartialPath fullPath,
      String className,
      Map<String, String> attributes,
      Class<?> triggerClass,
      boolean isStopped) {
    this.triggerName = triggerName;
    this.event = event;
    this.fullPath = fullPath;
    this.className = className;
    this.attributes = attributes;
    this.triggerClass = triggerClass;
    this.isStopped = isStopped;
  }

  public CreateTriggerPlan convertToCreateTriggerPlan() {
    return new CreateTriggerPlan(triggerName, event, fullPath, className, attributes);
  }

  public void updateTriggerClass(Class<?> triggerClass) {
    this.triggerClass = triggerClass;
  }

  public void markAsStarted() {
    isStopped = false;
  }

  public void markAsStopped() {
    isStopped = true;
  }

  public String getTriggerName() {
    return triggerName;
  }

  public TriggerEvent getEvent() {
    return event;
  }

  public PartialPath getFullPath() {
    return fullPath;
  }

  public String getClassName() {
    return className;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public Class<?> getTriggerClass() {
    return triggerClass;
  }

  public boolean isStopped() {
    return isStopped;
  }
}
