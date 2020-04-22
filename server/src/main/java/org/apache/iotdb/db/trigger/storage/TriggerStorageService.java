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

package org.apache.iotdb.db.trigger.storage;

import static org.apache.iotdb.db.trigger.storage.TriggerStorageUtil.createTriggerInstanceFromJar;
import static org.apache.iotdb.db.trigger.storage.TriggerStorageUtil.makeTriggerConfigurationFileIfNecessary;
import static org.apache.iotdb.db.trigger.storage.TriggerStorageUtil.makeTriggerStorageDirectoriesIfNecessary;
import static org.apache.iotdb.db.trigger.storage.TriggerStorageUtil.recoveryTriggersFromConfigurationFile;
import static org.apache.iotdb.db.trigger.storage.TriggerStorageUtil.registerTriggerToConfigurationFile;
import static org.apache.iotdb.db.trigger.storage.TriggerStorageUtil.removeTriggerFromConfigurationFile;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.trigger.TriggerInstanceLoadException;
import org.apache.iotdb.db.exception.trigger.TriggerManagementException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.trigger.define.Trigger;
import org.apache.iotdb.db.trigger.define.TriggerParameterConfiguration;

// todo: undo / redo ?
public class TriggerStorageService implements IService {

  private final Lock lock;

  private TriggerStorageService() {
    lock = new ReentrantLock();
  }

  @Override
  public void start() throws StartupException {
    if (!(makeTriggerStorageDirectoriesIfNecessary()
        && makeTriggerConfigurationFileIfNecessary())) {
      throw new StartupException("Could not create trigger storage files.");
    }
  }

  @Override
  public void stop() {
    lock.lock();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TRIGGER_STORAGE_SERVICE;
  }

  /**
   * @param path full path string of the time series. please make sure the time series is existed.
   */
  public Trigger createTrigger(String className, String path, String id, int enabledHooks,
      TriggerParameterConfiguration[] parameterConfigurations)
      throws TriggerInstanceLoadException, TriggerManagementException {
    lock.lock();
    try {
      Trigger trigger = createTriggerInstanceFromJar(className, path, id, enabledHooks,
          parameterConfigurations, true);
      registerTriggerToConfigurationFile(trigger);
      return trigger;
    } finally {
      lock.unlock();
    }
  }

  public void updateTrigger(Trigger trigger) throws TriggerManagementException {
    lock.lock();
    try {
      removeTriggerFromConfigurationFile(trigger);
      registerTriggerToConfigurationFile(trigger);
    } finally {
      lock.unlock();
    }
  }

  public void removeTrigger(Trigger trigger) throws TriggerManagementException {
    lock.lock();
    try {
      removeTriggerFromConfigurationFile(trigger);
    } finally {
      lock.unlock();
    }
  }

  public List<Trigger> recoveryAllTriggers() throws TriggerInstanceLoadException {
    lock.lock();
    try {
      return recoveryTriggersFromConfigurationFile();
    } finally {
      lock.unlock();
    }
  }

  public static TriggerStorageService getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    private static final TriggerStorageService INSTANCE = new TriggerStorageService();

    private InstanceHolder() {
    }
  }
}
