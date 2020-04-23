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

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.exception.trigger.TriggerInstanceLoadException;
import org.apache.iotdb.db.trigger.define.AsyncTrigger;
import org.apache.iotdb.db.trigger.storage.TriggerStorageUtil;

public class AsyncTriggerWrapper extends WrappedRunnable {

  private final AsyncTrigger handler;
  private AsyncTriggerJob job;

  public AsyncTriggerWrapper(AsyncTrigger trigger) {
    this.handler = trigger;
  }

  public static AsyncTriggerWrapper createHandler(AsyncTrigger trigger)
      throws TriggerInstanceLoadException {
    return new AsyncTriggerWrapper(
        (AsyncTrigger) TriggerStorageUtil.createTriggerInstanceFromJar(trigger));
  }

  @Override
  public void runMayThrow() throws Exception {
    switch (job.getHookID()) {
      case ON_DATA_POINT_BEFORE_INSERT:
        handler.onDataPointBeforeInsert(job.getTimestamp(), job.getValue());
      case ON_DATA_POINT_AFTER_INSERT:
        handler.onDataPointAfterInsert(job.getTimestamp(), job.getValue());
      case ON_BATCH_BEFORE_INSERT:
        handler.onBatchBeforeInsert(job.getTimestamps(), job.getValues());
      case ON_BATCH_AFTER_INSERT:
        handler.onBatchAfterInsert(job.getTimestamps(), job.getValues());
      case ON_DATA_POINT_BEFORE_DELETE:
        handler.onDataPointBeforeDelete(job.getTimestamp());
      case ON_DATA_POINT_AFTER_DELETE:
        handler.onDataPointAfterDelete(job.getTimestamp());
      default:
        throw new UnsupportedOperationException("Unsupported async trigger job.");
    }
  }

  public void setJob(AsyncTriggerJob job) {
    this.job = job;
  }
}
