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

import org.apache.iotdb.db.trigger.async.AsyncTriggerTask;

public abstract class AsyncTrigger extends Trigger {

  public AsyncTrigger(String path, String id, int enabledHooks,
      TriggerParameterConfigurations parameters, boolean isActive) {
    super(path, id, enabledHooks, parameters, isActive);
  }

  @Override
  public final boolean isSynced() {
    return false;
  }

  public void beforeInsert(final long timestamp, final Object value) {
  }

  public void afterInsert(final long timestamp, final Object value) {
  }

  public void beforeBatchInsert(final long[] timestamps, final Object[] values) {
  }

  public void afterBatchInsert(final long[] timestamps, final Object[] values) {
  }

  public void beforeDelete(final long timestamp) {
  }

  public void afterDelete(final long timestamp) {
  }

  public AsyncTriggerRejectionPolicy getRejectionPolicy(AsyncTriggerTask task) {
    switch (task.getHookID()) {
      case BEFORE_INSERT:
      case AFTER_INSERT:
      case BEFORE_BATCH_INSERT:
      case AFTER_BATCH_INSERT:
      case BEFORE_DELETE:
      case AFTER_DELETE:
      default:
        return AsyncTriggerRejectionPolicy.ENQUEUE;
    }
  }
}
