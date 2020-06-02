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

package org.apache.iotdb.db.trigger.manager;

import static org.apache.iotdb.db.trigger.definition.HookID.AFTER_DELETE;
import static org.apache.iotdb.db.trigger.definition.HookID.AFTER_INSERT_RECORD;
import static org.apache.iotdb.db.trigger.definition.HookID.AFTER_INSERT_TABLET;
import static org.apache.iotdb.db.trigger.definition.HookID.BEFORE_DELETE;
import static org.apache.iotdb.db.trigger.definition.HookID.BEFORE_INSERT_RECORD;
import static org.apache.iotdb.db.trigger.definition.HookID.BEFORE_INSERT_TABLET;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.trigger.TriggerException;
import org.apache.iotdb.db.exception.trigger.TriggerInstanceLoadException;
import org.apache.iotdb.db.exception.trigger.TriggerManagementException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.LeafMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.trigger.async.AsyncTriggerScheduler;
import org.apache.iotdb.db.trigger.async.AsyncTriggerTask;
import org.apache.iotdb.db.trigger.definition.AsyncTrigger;
import org.apache.iotdb.db.trigger.definition.SyncTrigger;
import org.apache.iotdb.db.trigger.definition.Trigger;
import org.apache.iotdb.db.trigger.definition.TriggerParameterConfigurations;
import org.apache.iotdb.db.trigger.storage.TriggerStorageService;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(TriggerManager.class);

  private final ConcurrentMap<String, Trigger> idToTriggers;
  private final ConcurrentMap<String, Trigger> pathToSyncTriggers;
  private final ConcurrentMap<String, Trigger> pathToAsyncTriggers;

  private TriggerManager() {
    idToTriggers = new ConcurrentHashMap<>();
    pathToSyncTriggers = new ConcurrentHashMap<>();
    pathToAsyncTriggers = new ConcurrentHashMap<>();
  }

  @Override
  public void start() throws StartupException {
    try {
      initMapsAndStartTriggers(TriggerStorageService.getInstance().recoveryAllTriggers());
      logger.info("TriggerManager service started.");
    } catch (TriggerException e) {
      throw new StartupException(getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    stopTriggers();
    logger.info("TriggerManager service stopped.");
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TRIGGER_MANAGER_SERVICE;
  }

  /**
   * NOTICE: {@code insertPlan} may be modified in this method.
   *
   * @return Return {@code true} to continue the insert operation. Return {@code false} to terminate
   * the insert operation.
   */
  public boolean fireBeforeInsertRecord(InsertPlan insertPlan) {
    List<Path> paths = insertPlan.getPaths();
    long timestamp = insertPlan.getTime();
    Object[] values = insertPlan.getValues();
    Object[] newValues = values;

    for (int i = 0; i < paths.size(); ++i) {
      String path = paths.get(i).getFullPath();
      Object value = values[i];
      // fire async triggers
      AsyncTrigger asyncTrigger = (AsyncTrigger) pathToAsyncTriggers.get(path);
      if (asyncTrigger != null && asyncTrigger.isActive() && BEFORE_INSERT_RECORD
          .isEnabled(asyncTrigger.getEnabledHooks()) && asyncTrigger
          .conditionBeforeInsertRecord(timestamp, value)) {
        AsyncTriggerScheduler.getInstance().submit(
            new AsyncTriggerTask(asyncTrigger, BEFORE_INSERT_RECORD, timestamp, value, null, null));
      }
      // fire sync triggers
      SyncTrigger syncTrigger = (SyncTrigger) pathToSyncTriggers.get(path);
      if (syncTrigger != null && syncTrigger.isActive() && BEFORE_INSERT_RECORD
          .isEnabled(syncTrigger.getEnabledHooks()) && syncTrigger
          .conditionBeforeInsertRecord(timestamp, value)) {
        values[i] = syncTrigger.actionBeforeInsertRecord(timestamp, value);
        if (values[i] == null) {
          newValues = null;
        }
      }
    }

    insertPlan.setValues(newValues);
    return newValues != null;
  }

  public void fireAfterInsertRecord(InsertPlan insertPlan) {
    List<Path> paths = insertPlan.getPaths();
    long timestamp = insertPlan.getTime();
    Object[] values = insertPlan.getValues();

    for (int i = 0; i < paths.size(); ++i) {
      String path = paths.get(i).getFullPath();
      Object value = values[i];
      // fire async triggers
      AsyncTrigger asyncTrigger = (AsyncTrigger) pathToAsyncTriggers.get(path);
      if (asyncTrigger != null && asyncTrigger.isActive() && AFTER_INSERT_RECORD
          .isEnabled(asyncTrigger.getEnabledHooks()) && asyncTrigger
          .conditionAfterInsertRecord(timestamp, value)) {
        AsyncTriggerScheduler.getInstance().submit(
            new AsyncTriggerTask(asyncTrigger, AFTER_INSERT_RECORD, timestamp, value, null, null));
      }
      // fire sync triggers
      SyncTrigger syncTrigger = (SyncTrigger) pathToSyncTriggers.get(path);
      if (syncTrigger != null && syncTrigger.isActive() && AFTER_INSERT_RECORD
          .isEnabled(syncTrigger.getEnabledHooks()) && syncTrigger
          .conditionAfterInsertRecord(timestamp, value)) {
        syncTrigger.actionAfterInsertRecord(timestamp, value);
      }
    }
  }

  /**
   * @return Return the actual timestamp in the delete operation. Return {@code null} to terminate
   * the delete operation.
   */
  public Long fireBeforeDelete(Path path, long timestamp) {
    Long ret = timestamp;
    // fire async trigger
    AsyncTrigger asyncTrigger = (AsyncTrigger) pathToAsyncTriggers.get(path.getFullPath());
    if (asyncTrigger != null && asyncTrigger.isActive() && BEFORE_DELETE
        .isEnabled(asyncTrigger.getEnabledHooks()) && asyncTrigger
        .conditionBeforeDelete(timestamp)) {
      AsyncTriggerScheduler.getInstance()
          .submit(new AsyncTriggerTask(asyncTrigger, BEFORE_DELETE, timestamp, null, null, null));
    }
    // fire sync trigger
    SyncTrigger syncTrigger = (SyncTrigger) pathToSyncTriggers.get(path.getFullPath());
    if (syncTrigger != null && syncTrigger.isActive() && BEFORE_DELETE
        .isEnabled(syncTrigger.getEnabledHooks()) && syncTrigger.conditionBeforeDelete(timestamp)) {
      ret = syncTrigger.actionBeforeDelete(timestamp);
    }
    return ret;
  }

  public void fireAfterDelete(Path path, long timestamp) {
    // fire async trigger
    AsyncTrigger asyncTrigger = (AsyncTrigger) pathToAsyncTriggers.get(path.getFullPath());
    if (asyncTrigger != null && asyncTrigger.isActive() && AFTER_DELETE
        .isEnabled(asyncTrigger.getEnabledHooks()) && asyncTrigger
        .conditionAfterDelete(timestamp)) {
      AsyncTriggerScheduler.getInstance()
          .submit(new AsyncTriggerTask(asyncTrigger, AFTER_DELETE, timestamp, null, null, null));
    }
    // fire sync trigger
    SyncTrigger syncTrigger = (SyncTrigger) pathToSyncTriggers.get(path.getFullPath());
    if (syncTrigger != null && syncTrigger.isActive() && AFTER_DELETE
        .isEnabled(syncTrigger.getEnabledHooks()) && syncTrigger.conditionAfterDelete(timestamp)) {
      syncTrigger.actionAfterDelete(timestamp);
    }
  }

  /**
   * NOTICE: {@code insertTabletPlan} may be modified in this method.
   *
   * @return Return {@code true} to continue the insert operation. Return {@code false} to terminate
   * the insert operation.
   */
  public boolean fireBeforeInsertTablet(InsertTabletPlan insertTabletPlan) {
    List<Path> paths = insertTabletPlan.getPaths();
    long[] timestamps = insertTabletPlan.getTimes();
    Object[] columns = insertTabletPlan.getColumns();
    Object[] newColumns = columns;

    for (int i = 0; i < paths.size(); ++i) {
      String path = paths.get(i).getFullPath();
      Object values = columns[i];
      // fire async triggers
      AsyncTrigger asyncTrigger = (AsyncTrigger) pathToAsyncTriggers.get(path);
      if (asyncTrigger != null && asyncTrigger.isActive() && BEFORE_INSERT_TABLET
          .isEnabled(asyncTrigger.getEnabledHooks()) && asyncTrigger
          .conditionBeforeInsertTablet(timestamps, values)) {
        AsyncTriggerScheduler.getInstance().submit(
            new AsyncTriggerTask(asyncTrigger, BEFORE_INSERT_TABLET, -1, null, timestamps, values));
      }
      // fire sync triggers
      SyncTrigger syncTrigger = (SyncTrigger) pathToSyncTriggers.get(path);
      if (syncTrigger != null && syncTrigger.isActive() && BEFORE_INSERT_TABLET
          .isEnabled(syncTrigger.getEnabledHooks()) && syncTrigger
          .conditionBeforeInsertTablet(timestamps, values)) {
        columns[i] = syncTrigger.actionBeforeInsertTablet(timestamps, values);
        if (columns[i] == null) {
          newColumns = null;
        }
      }
    }

    insertTabletPlan.setColumns(newColumns);
    return newColumns != null;
  }

  public void fireAfterInsertTablet(InsertTabletPlan insertTabletPlan) {
    List<Path> paths = insertTabletPlan.getPaths();
    long[] timestamps = insertTabletPlan.getTimes();
    Object[] columns = insertTabletPlan.getColumns();

    for (int i = 0; i < paths.size(); ++i) {
      String path = paths.get(i).getFullPath();
      Object values = columns[i];
      // fire async triggers
      AsyncTrigger asyncTrigger = (AsyncTrigger) pathToAsyncTriggers.get(path);
      if (asyncTrigger != null && asyncTrigger.isActive() && AFTER_INSERT_TABLET
          .isEnabled(asyncTrigger.getEnabledHooks()) && asyncTrigger
          .conditionAfterInsertTablet(timestamps, values)) {
        AsyncTriggerScheduler.getInstance().submit(
            new AsyncTriggerTask(asyncTrigger, AFTER_INSERT_TABLET, -1, null, timestamps, values));
      }
      // fire sync triggers
      SyncTrigger syncTrigger = (SyncTrigger) pathToSyncTriggers.get(path);
      if (syncTrigger != null && syncTrigger.isActive() && AFTER_INSERT_TABLET
          .isEnabled(syncTrigger.getEnabledHooks()) && syncTrigger
          .conditionAfterInsertTablet(timestamps, values)) {
        syncTrigger.actionAfterInsertTablet(timestamps, values);
      }
    }
  }

  public List<Trigger> show(String path, boolean showSyncTrigger, boolean showAsyncTrigger) {
    List<Trigger> triggers = new ArrayList<>();
    if (path == null) {
      if (showSyncTrigger) {
        triggers.addAll(pathToSyncTriggers.values());
      }
      if (showAsyncTrigger) {
        triggers.addAll(pathToAsyncTriggers.values());
      }
    } else {
      if (showSyncTrigger) {
        Trigger trigger = pathToSyncTriggers.get(path);
        if (trigger != null) {
          triggers.add(trigger);
        }
      }
      if (showAsyncTrigger) {
        Trigger trigger = pathToAsyncTriggers.get(path);
        if (trigger != null) {
          triggers.add(trigger);
        }
      }
    }
    return triggers;
  }

  public void create(String className, String path, String id, int enabledHooks,
      TriggerParameterConfigurations parameterConfigurations)
      throws TriggerInstanceLoadException, TriggerManagementException, MetadataException {
    checkPath(path);
    Trigger trigger = TriggerStorageService.getInstance()
        .createTrigger(className, path, id, enabledHooks, parameterConfigurations);
    trigger.beforeStart();
    if (trigger.isSynced()) {
      pathToSyncTriggers.put(trigger.getPath(), trigger);
    } else {
      AsyncTriggerScheduler.getInstance().beforeTriggerStart((AsyncTrigger) trigger);
      pathToAsyncTriggers.put(trigger.getPath(), trigger);
    }
    idToTriggers.put(trigger.getId(), trigger);
  }

  public void start(String id) throws TriggerManagementException, TriggerInstanceLoadException {
    Trigger trigger = idToTriggers.get(id);
    if (trigger == null) {
      throw new TriggerManagementException(String
          .format("Could not start Trigger(ID: %s), because the trigger does not exist.", id));
    }
    if (trigger.isActive()) {
      throw new TriggerManagementException(String
          .format("Trigger(ID: %s) has already been started.", id));
    }
    trigger.beforeStart();
    if (!trigger.isSynced()) {
      AsyncTriggerScheduler.getInstance().beforeTriggerStart((AsyncTrigger) trigger);
    }
    trigger.markAsActive();
    TriggerStorageService.getInstance().updateTrigger(trigger);
  }

  public void stop(String id) throws TriggerManagementException {
    Trigger trigger = idToTriggers.get(id);
    if (trigger == null) {
      throw new TriggerManagementException(String
          .format("Could not stop trigger(ID: %s), because the trigger does not exist.", id));
    }
    if (!trigger.isActive()) {
      throw new TriggerManagementException(String
          .format("Trigger(ID: %s) has already been stopped.", id));
    }
    trigger.markAsInactive();
    TriggerStorageService.getInstance().updateTrigger(trigger);
    if (!trigger.isSynced()) {
      AsyncTriggerScheduler.getInstance().afterTriggerStop((AsyncTrigger) trigger);
    }
    trigger.afterStop();
  }

  public void removeById(String id) throws TriggerManagementException {
    Trigger trigger = idToTriggers.get(id);
    if (trigger == null) {
      throw new TriggerManagementException(String
          .format("Could not remove Trigger(ID: %s), because the trigger does not exist.", id));
    }
    if (trigger.isActive()) {
      trigger.markAsInactive();
      trigger.afterStop();
      if (!trigger.isSynced()) {
        AsyncTriggerScheduler.getInstance().afterTriggerStop((AsyncTrigger) trigger);
      }
    }
    TriggerStorageService.getInstance().removeTrigger(trigger);
    idToTriggers.remove(id);
    if (trigger.isSynced()) {
      pathToSyncTriggers.remove(trigger.getPath());
    } else {
      pathToAsyncTriggers.remove(trigger.getPath());
    }
  }

  public void removeByPath(String path) throws TriggerManagementException {
    removeByPath(path, pathToSyncTriggers);
    removeByPath(path, pathToAsyncTriggers);
  }

  private void removeByPath(String path, ConcurrentMap<String, Trigger> map)
      throws TriggerManagementException {
    Trigger trigger = map.get(path);
    if (trigger == null) {
      logger.info("Could not remove {} trigger(path: {}), because the trigger does not exist.",
          map == pathToSyncTriggers ? "sync" : "async", path);
      return;
    }
    if (trigger.isActive()) {
      trigger.markAsInactive();
      trigger.afterStop();
      if (!trigger.isSynced()) {
        AsyncTriggerScheduler.getInstance().afterTriggerStop((AsyncTrigger) trigger);
      }
    }
    TriggerStorageService.getInstance().removeTrigger(trigger);
    idToTriggers.remove(trigger.getId());
    map.remove(trigger.getPath());
    logger.info("{} trigger(path: {}) has been removed successfully.",
        map == pathToSyncTriggers ? "Sync" : "Async", path);
  }

  private void initMapsAndStartTriggers(List<Trigger> triggers) {
    for (Trigger trigger : triggers) {
      if (trigger.isActive()) {
        trigger.beforeStart();
      }
      idToTriggers.put(trigger.getId(), trigger);
      if (trigger.isSynced()) {
        pathToSyncTriggers.put(trigger.getPath(), trigger);
      } else {
        pathToAsyncTriggers.put(trigger.getPath(), trigger);
      }
    }
  }

  private void stopTriggers() {
    for (Trigger trigger : idToTriggers.values()) {
      if (trigger.isActive()) {
        trigger.afterStop();
      }
    }
  }

  private void checkPath(String pathString) throws MetadataException {
    MNode node = MManager.getInstance().getNodeByPath(pathString);
    if (!(node instanceof LeafMNode)) {
      throw new PathNotExistException(String.format("%s is not a measurement path.", pathString));
    }
  }

  public static TriggerManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static final TriggerManager INSTANCE = new TriggerManager();
  }
}
