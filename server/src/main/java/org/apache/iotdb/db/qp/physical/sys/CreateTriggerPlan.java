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

package org.apache.iotdb.db.qp.physical.sys;

import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.trigger.definition.TriggerParameterConfigurations;
import org.apache.iotdb.tsfile.read.common.Path;

public class CreateTriggerPlan extends PhysicalPlan {

  private final String className;
  private final String path;
  private final String id;
  private final int enabledHooks;
  private final TriggerParameterConfigurations parameterConfigurations;

  public CreateTriggerPlan(String className, String path, String id, int enabledHooks,
      TriggerParameterConfigurations parameterConfigurations) {
    super(false, OperatorType.CREATE_TRIGGER);
    this.className = className;
    this.path = path;
    this.id = id;
    this.enabledHooks = enabledHooks;
    this.parameterConfigurations = parameterConfigurations;
    canbeSplit = false;
  }

  @Override
  public List<Path> getPaths() {
    return Collections.emptyList();
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
    return enabledHooks;
  }

  public TriggerParameterConfigurations getParameterConfigurations() {
    return parameterConfigurations;
  }
}
