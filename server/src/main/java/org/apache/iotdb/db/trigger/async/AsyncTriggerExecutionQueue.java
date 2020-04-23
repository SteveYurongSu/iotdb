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

import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.iotdb.db.exception.trigger.TriggerInstanceLoadException;
import org.apache.iotdb.db.trigger.define.AsyncTrigger;
import org.apache.iotdb.db.trigger.define.AsyncTriggerRejectionPolicy;

public class AsyncTriggerExecutorQueue {

  private final static int MAX_ASYNC_TRIGGER_EXECUTOR = 10;

  private final AsyncTrigger trigger;
  private final ConcurrentLinkedQueue<AsyncTriggerExecutor> executors;
  private final ConcurrentLinkedQueue<AsyncTriggerJob> jobs;
  private final Object pollLock;

  public AsyncTriggerExecutorQueue(AsyncTrigger trigger) throws TriggerInstanceLoadException {
    this.trigger = trigger;
    executors = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < MAX_ASYNC_TRIGGER_EXECUTOR; ++i) {
      executors.add(AsyncTriggerExecutor.createExecutor(trigger));
    }
    jobs = new ConcurrentLinkedQueue<>();
    pollLock = new Object();
  }

  public boolean enqueueJob(AsyncTriggerJob job) throws TriggerInstanceLoadException {
    if (executors.size() == 0 && trigger.getRejectionPolicy(job.getHookID())
        .equals(AsyncTriggerRejectionPolicy.DISCARD)) {
      return false;
    }
    jobs.add(job);
    return true;
  }

  public AsyncTriggerExecutor pollExecutor() {
    synchronized (pollLock) {
      if (executors.size() == 0 || jobs.size() == 0) {
        return null;
      }
      AsyncTriggerExecutor executor = executors.poll();
      executor.setJob(jobs.poll());
      return executor;
    }
  }

  public void releaseExecutor(AsyncTriggerExecutor executor) {
    executors.add(executor);
  }

  public int getQueuedJobSize() {
    return jobs.size();
  }
}
