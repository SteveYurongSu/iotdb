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

package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.trigger.definition.HookID;
import org.apache.iotdb.db.trigger.definition.TriggerParameterConfigurations;

public class CreateTriggerOperator extends RootOperator {

  private String className;
  private String path;
  private String id;
  private int enabledHooks;
  private TriggerParameterConfigurations parameterConfigurations;

  public CreateTriggerOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.CREATE_TRIGGER;
    enabledHooks = 0B0;
    parameterConfigurations = new TriggerParameterConfigurations();
  }

  public void addParameter(String key, String value) {
    parameterConfigurations.put(key, value);
  }

  public void enableHook(HookID hook) {
    enabledHooks = enabledHooks | hook.getId();
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setEnabledHooks(int enabledHooks) {
    this.enabledHooks = enabledHooks;
  }

  public void setParameterConfigurations(
      TriggerParameterConfigurations parameterConfigurations) {
    this.parameterConfigurations = parameterConfigurations;
  }

  public String getClassName() {
    return className;
  }

  public String getPath() {
    return path;
  }

  public String getId() {
    return id;
  }

  public int getEnabledHooks() {
    // default 'ON ALL EVENTS'
    return enabledHooks == 0B0 ? HookID.ON_ALL_EVENTS.getId() : enabledHooks;
  }

  public TriggerParameterConfigurations getParameterConfigurations() {
    return parameterConfigurations;
  }
}
