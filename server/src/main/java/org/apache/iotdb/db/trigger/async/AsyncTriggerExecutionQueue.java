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
import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.exception.trigger.TriggerInstanceLoadException;
import org.apache.iotdb.db.trigger.define.AsyncTrigger;
import org.apache.iotdb.db.trigger.define.AsyncTriggerRejectionPolicy;

public class AsyncTriggerExecutionQueue extends WrappedRunnable {

  private final static int MAX_ASYNC_TRIGGER_EXECUTOR = 10;

  private final AsyncTrigger trigger;
  private final ConcurrentLinkedQueue<AsyncTriggerExecutor> executors;
  private final ConcurrentLinkedQueue<AsyncTriggerTask> tasks;

  public AsyncTriggerExecutionQueue(AsyncTrigger trigger) throws TriggerInstanceLoadException {
    this.trigger = trigger;
    executors = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < MAX_ASYNC_TRIGGER_EXECUTOR; ++i) {
      executors.add(new AsyncTriggerExecutor(trigger));
    }
    tasks = new ConcurrentLinkedQueue<>();
  }

  public boolean submit(AsyncTriggerTask task) {
    if (executors.isEmpty() && trigger.getRejectionPolicy(task.getHookID())
        .equals(AsyncTriggerRejectionPolicy.DISCARD)) {
      return false;
    }
    tasks.add(task);
    return true;
  }

  public synchronized AsyncTriggerExecutor pollExecutor() {
    if (executors.isEmpty() || tasks.isEmpty()) {
      return null;
    }
    AsyncTriggerExecutor executor = executors.poll();
    executor.setTask(tasks.poll());
    return executor;
  }

  public void releaseExecutor(AsyncTriggerExecutor executor) {
    executors.add(executor);
  }

  /**
   * enter the method only when hasQueuedTasks() && !allExecutorsAreBusy()
   */
  @Override
  public void runMayThrow() throws Exception {
    AsyncTriggerExecutor executor = pollExecutor();
    if (executor == null) {
      return;
    }
    try {
      executor.execute();
    } finally {
      releaseExecutor(executor);
    }
  }

  public void stop() {
    while (hasQueuedTasks()) {
      tasks.poll();
    }
    while (!allExecutorsAreIdle()) {
      ;
    }
    executors.forEach(AsyncTriggerExecutor::afterStop);
  }

  public boolean hasQueuedTasks() {
    return !tasks.isEmpty();
  }

  public boolean allExecutorsAreBusy() {
    return executors.isEmpty();
  }

  private boolean allExecutorsAreIdle() {
    return executors.size() == MAX_ASYNC_TRIGGER_EXECUTOR;
  }
}
