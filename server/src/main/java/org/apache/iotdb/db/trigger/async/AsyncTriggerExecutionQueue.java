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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.trigger.TriggerInstanceLoadException;
import org.apache.iotdb.db.trigger.definition.AsyncTrigger;
import org.apache.iotdb.db.trigger.definition.AsyncTriggerRejectionPolicy;

public class AsyncTriggerExecutionQueue extends WrappedRunnable {

  private final ConcurrentLinkedQueue<AsyncTriggerExecutor> idleExecutors;
  private final ConcurrentLinkedQueue<AsyncTriggerExecutor> busyExecutors;

  private final int maxTaskNumber;
  private final AtomicInteger taskNumber;
  private final ConcurrentLinkedQueue<AsyncTriggerTask> tasks;

  public AsyncTriggerExecutionQueue(AsyncTrigger trigger) throws TriggerInstanceLoadException {
    int maxAsyncTriggerExecutorNumber = IoTDBDescriptor.getInstance().getConfig()
        .getAsyncTriggerTaskExecutorNum();
    idleExecutors = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < maxAsyncTriggerExecutorNumber; ++i) {
      idleExecutors.add(new AsyncTriggerExecutor(trigger));
    }
    busyExecutors = new ConcurrentLinkedQueue<>();
    maxTaskNumber = IoTDBDescriptor.getInstance().getConfig()
        .getMaxQueuedAsyncTriggerTasksNumForEachTriggerInstance();
    taskNumber = new AtomicInteger(0);
    tasks = new ConcurrentLinkedQueue<>();
  }

  public boolean submit(AsyncTriggerTask task) {
    if (maxTaskNumber < taskNumber.get() && task.getRejectionPolicy()
        .equals(AsyncTriggerRejectionPolicy.DISCARD)) {
      return false;
    }
    addTask(task);
    return true;
  }

  public boolean getReadyForSubmit() {
    if (allExecutorsAreBusy()) {
      return false;
    }
    AsyncTriggerExecutor executor = idleExecutors.poll();
    executor.setTask(pollTask());
    busyExecutors.add(executor);
    return true;
  }

  @Override
  public void runMayThrow() throws Exception {
    AsyncTriggerExecutor executor = busyExecutors.poll();
    try {
      executor.execute();
    } finally {
      idleExecutors.add(executor);
    }
  }

  public void afterTriggerStop() {
    while (!allExecutorsAreIdle()) {
      ; // wait for all submitted tasks to complete execution
    }
    while (hasQueuedTasks()) {
      pollTask(); // ignore the tasks which are not submitted to the pool
    }
    idleExecutors.forEach(AsyncTriggerExecutor::afterTriggerStop);
  }

  public boolean hasQueuedTasks() {
    return !tasks.isEmpty();
  }

  public boolean allExecutorsAreBusy() {
    return idleExecutors.isEmpty();
  }

  public boolean allExecutorsAreIdle() {
    return busyExecutors.isEmpty();
  }

  private void addTask(AsyncTriggerTask task) {
    taskNumber.incrementAndGet();
    tasks.add(task);
  }

  private AsyncTriggerTask pollTask() {
    taskNumber.decrementAndGet();
    return tasks.poll();
  }
}
