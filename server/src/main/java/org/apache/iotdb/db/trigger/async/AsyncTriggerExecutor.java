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
import org.apache.iotdb.db.trigger.definition.AsyncTrigger;
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
      case BEFORE_INSERT:
        handler.beforeInsert(task.getTimestamp(), task.getValue());
        return;
      case AFTER_INSERT:
        handler.afterInsert(task.getTimestamp(), task.getValue());
        return;
      case BEFORE_BATCH_INSERT:
        handler.beforeBatchInsert(task.getTimestamps(), task.getValues());
        return;
      case AFTER_BATCH_INSERT:
        handler.afterBatchInsert(task.getTimestamps(), task.getValues());
        return;
      case BEFORE_DELETE:
        handler.beforeDelete(task.getTimestamp());
        return;
      case AFTER_DELETE:
        handler.afterDelete(task.getTimestamp());
        return;
      default:
        throw new UnsupportedOperationException("Unsupported async trigger task.");
    }
  }

  public void afterTriggerStop() {
    handler.afterStop();
  }

  public void setTask(AsyncTriggerTask task) {
    this.task = task;
  }
}
