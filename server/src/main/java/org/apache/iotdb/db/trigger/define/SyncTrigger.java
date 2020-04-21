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

package org.apache.iotdb.db.trigger.define;

import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;

public abstract class SyncTrigger extends Trigger {

  public SyncTrigger(String path, String id, TriggerParameterConfiguration[] parameters,
      boolean isActive) {
    super(path, id, parameters, isActive);
  }

  @Override
  public final boolean isSynced() {
    return true;
  }

  public TriggerExecutionResult onDataPointBeforeInsert(final long timestamp,
      final DataPoint value) {
    return TriggerExecutionResult.DATA_POINT_NOT_CHANGED;
  }

  public TriggerExecutionResult onBatchBeforeInsert(final long[] timestamps,
      final Object[] values) {
    return TriggerExecutionResult.DATA_POINT_NOT_CHANGED;
  }

  public TriggerExecutionResult onDataPointBeforeDelete(final long timestamp) {
    return TriggerExecutionResult.DATA_POINT_NOT_CHANGED;
  }
}
