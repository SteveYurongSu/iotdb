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

public abstract class AsyncTrigger extends Trigger {

  public AsyncTrigger(String path, String id, int enabledHooks,
      TriggerParameterConfiguration[] parameters, boolean isActive) {
    super(path, id, enabledHooks, parameters, isActive);
  }

  @Override
  public final boolean isSynced() {
    return false;
  }

  public void onDataPointBeforeInsert(final long timestamp, final Object value) {
  }

  public void onBatchBeforeInsert(final long[] timestamps, final Object[] values) {
  }

  public void onDataPointBeforeDelete(final long timestamp) {
  }

  public void onDataPointAfterInsert(final long timestamp, final Object value) {
  }

  public void onBatchAfterInsert(final long[] timestamps, final Object[] values) {
  }

  public void onDataPointAfterDelete(final long timestamp) {
  }

  public AsyncTriggerRejectionPolicy getRejectionPolicy(HookID hook) {
    switch (hook) {
      case ON_DATA_POINT_BEFORE_INSERT:
      case ON_DATA_POINT_BEFORE_DELETE:
      case ON_DATA_POINT_AFTER_INSERT:
      case ON_DATA_POINT_AFTER_DELETE:
      case ON_BATCH_BEFORE_INSERT:
      case ON_BATCH_AFTER_INSERT:
      default:
        return AsyncTriggerRejectionPolicy.ENQUEUE;
    }
  }
}
