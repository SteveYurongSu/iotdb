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

package org.apache.iotdb.db.trigger.async;

import org.apache.iotdb.db.exception.trigger.TriggerInstanceLoadException;
import org.apache.iotdb.db.trigger.define.AsyncTrigger;
import org.apache.iotdb.db.trigger.storage.TriggerStorageUtil;

public class AsyncTriggerExecutor {

  private final AsyncTrigger handler;
  private AsyncTriggerTask task;

  public AsyncTriggerExecutor(AsyncTrigger trigger) throws TriggerInstanceLoadException {
    handler = (AsyncTrigger) TriggerStorageUtil.createTriggerInstanceFromJar(trigger);
    handler.beforeStart();
  }

  public void execute() {
    switch (task.getHookID()) {
      case ON_DATA_POINT_BEFORE_INSERT:
        handler.onDataPointBeforeInsert(task.getTimestamp(), task.getValue());
      case ON_DATA_POINT_AFTER_INSERT:
        handler.onDataPointAfterInsert(task.getTimestamp(), task.getValue());
      case ON_BATCH_BEFORE_INSERT:
        handler.onBatchBeforeInsert(task.getTimestamps(), task.getValues());
      case ON_BATCH_AFTER_INSERT:
        handler.onBatchAfterInsert(task.getTimestamps(), task.getValues());
      case ON_DATA_POINT_BEFORE_DELETE:
        handler.onDataPointBeforeDelete(task.getTimestamp());
      case ON_DATA_POINT_AFTER_DELETE:
        handler.onDataPointAfterDelete(task.getTimestamp());
      default:
        throw new UnsupportedOperationException("Unsupported async trigger task.");
    }
  }

  public void afterStop() {
    handler.afterStop();
  }

  public void setTask(AsyncTriggerTask task) {
    this.task = task;
  }
}
