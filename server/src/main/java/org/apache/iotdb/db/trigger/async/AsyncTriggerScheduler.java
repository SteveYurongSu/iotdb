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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.trigger.TriggerInstanceLoadException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.trigger.definition.AsyncTrigger;
import org.apache.iotdb.db.trigger.definition.AsyncTrigger.RejectionPolicy;
import org.apache.iotdb.db.trigger.definition.Trigger;
import org.apache.iotdb.db.trigger.storage.TriggerStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncTriggerScheduler implements IService {

  private static final Logger logger = LoggerFactory.getLogger(AsyncTriggerScheduler.class);

  private final ConcurrentHashMap<String, AsyncTriggerExecutionQueue> idToExecutionQueue;
  private LinkedBlockingQueue<AsyncTriggerExecutionQueue> waitingQueue;
  private AtomicInteger waitingTaskNumber;
  private final int maxWaitingTaskNumber;

  private ExecutorService executorService;

  private Thread transferThread;
  private final Runnable transferTask = () -> {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        AsyncTriggerExecutionQueue executionQueue = waitingQueue.take();
        if (!executionQueue.getReadyForSubmit()) {
          waitingQueue.offer(executionQueue);
        }
        executorService.submit(executionQueue);
        waitingTaskNumber.decrementAndGet();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    logger.info("AsyncTriggerScheduler transferThread exits.");
  };

  private AsyncTriggerScheduler() {
    idToExecutionQueue = new ConcurrentHashMap<>();
    waitingQueue = new LinkedBlockingQueue<>();
    waitingTaskNumber = new AtomicInteger(0);
    maxWaitingTaskNumber = IoTDBDescriptor.getInstance().getConfig()
        .getMaxQueuedAsyncTriggerTasksNum();
    executorService = IoTDBThreadPoolFactory.newFixedThreadPool(
        IoTDBDescriptor.getInstance().getConfig().getAsyncTriggerExecutionPoolSize(),
        ThreadName.TRIGGER_EXECUTOR_SERVICE.getName());
  }

  @Override
  public void start() throws StartupException {
    try {
      init();
      startTransferThread();
    } catch (TriggerInstanceLoadException e) {
      throw new StartupException(getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    stopTransferThread();
    executorService.shutdown();
    idToExecutionQueue.values().forEach(AsyncTriggerExecutionQueue::afterTriggerStop);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TRIGGER_ASYNC_SCHEDULER_SERVICE;
  }

  public void beforeTriggerStart(AsyncTrigger trigger) throws TriggerInstanceLoadException {
    idToExecutionQueue.put(trigger.getId(), new AsyncTriggerExecutionQueue(trigger));
  }

  public void afterTriggerStop(AsyncTrigger trigger) {
    idToExecutionQueue.remove(trigger.getId()).afterTriggerStop();
  }

  public void submit(AsyncTriggerTask task) {
    AsyncTriggerExecutionQueue executionQueue = idToExecutionQueue.get(task.getTrigger().getId());
    if (executionQueue == null) {
      return;
    }
    if (maxWaitingTaskNumber < waitingTaskNumber.get() && task.getRejectionPolicy()
        .equals(RejectionPolicy.DISCARD)) {
      return;
    }
    if (executionQueue.submit(task)) {
      waitingQueue.offer(executionQueue);
      waitingTaskNumber.incrementAndGet();
    }
  }

  private void init() throws TriggerInstanceLoadException {
    List<Trigger> triggers = TriggerStorageService.getInstance().recoveryAllTriggers();
    for (Trigger trigger : triggers) {
      if (!trigger.isSynced()) {
        idToExecutionQueue
            .put(trigger.getId(), new AsyncTriggerExecutionQueue((AsyncTrigger) trigger));
      }
    }
  }

  private boolean transferThreadIsActivated() {
    return transferThread != null && transferThread.isAlive();
  }

  private void startTransferThread() {
    if (!transferThreadIsActivated()) {
      transferThread = new Thread(transferTask, ThreadName.TRIGGER_SCHEDULER.getName());
      transferThread.start();
      logger.info("AsyncTriggerScheduler transferThread started.");
    } else {
      logger.warn("AsyncTriggerScheduler transferThread has already started.");
    }
  }

  private void stopTransferThread() {
    if (!transferThreadIsActivated()) {
      logger.warn("AsyncTriggerScheduler transferThread has not started yet.");
      return;
    }
    transferThread.interrupt();
    while (transferThread.isAlive()) {
      ;
    }
    logger.info("AsyncTriggerScheduler transferThread was interrupted.");
  }

  public static AsyncTriggerScheduler getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    private static final AsyncTriggerScheduler INSTANCE = new AsyncTriggerScheduler();

    private InstanceHolder() {
    }
  }
}
