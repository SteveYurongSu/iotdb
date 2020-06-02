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

  public enum RejectionPolicy {
    DISCARD, ENQUEUE
  }

  public AsyncTrigger(String path, String id, int enabledHooks,
      TriggerParameterConfigurations parameters, boolean isActive) {
    super(path, id, enabledHooks, parameters, isActive);
  }

  @Override
  public final boolean isSynced() {
    return false;
  }

  /**
   * This hook will be called if {@link Trigger#conditionBeforeInsertRecord(long, Object)} returns
   * {@code true}.
   *
   * @param timestamp The timestamp of the data point to be inserted.
   * @param value The value of the data point to be inserted.
   */
  public void actionBeforeInsertRecord(final long timestamp, final Object value) {
  }

  /**
   * This hook will be called if {@link Trigger#conditionAfterInsertRecord(long, Object)} returns
   * {@code true}.
   *
   * @param timestamp The timestamp of the inserted data point.
   * @param value The value of the inserted data point.
   */
  public void actionAfterInsertRecord(final long timestamp, final Object value) {
  }

  /**
   * This hook will be called if {@link Trigger#conditionBeforeInsertTablet(long[], Object)}
   * returns {@code true}. Modify params in this method may cause undefined behavior.
   *
   * @param timestamps All timestamps in the tablet to be inserted.
   * @param values All values in the tablet to be inserted.
   */
  public void actionBeforeInsertTablet(final long[] timestamps, final Object values) {
  }

  /**
   * This hook will be called if {@link Trigger#conditionAfterInsertTablet(long[], Object)}
   * returns {@code true}. Modify params in this method may cause undefined behavior.
   *
   * @param timestamps All timestamps in the inserted tablet.
   * @param values All values in the inserted tablet.
   */
  public void actionAfterInsertTablet(final long[] timestamps, final Object values) {
  }

  /**
   * This hook will be called if {@link Trigger#conditionBeforeDelete(long)} returns {@code true}.
   *
   * @param timestamp The timestamp in the delete operation.
   */
  public void actionBeforeDelete(final long timestamp) {
  }

  /**
   * This hook will be called if {@link Trigger#conditionAfterDelete(long)} returns {@code true}.
   *
   * @param timestamp The timestamp in the delete operation.
   */
  public void actionAfterDelete(final long timestamp) {
  }

  public RejectionPolicy rejectionPolicy(AsyncTriggerTask task) {
    switch (task.getHookID()) {
      case BEFORE_INSERT_RECORD:
      case AFTER_INSERT_RECORD:
      case BEFORE_INSERT_TABLET:
      case AFTER_INSERT_TABLET:
      case BEFORE_DELETE:
      case AFTER_DELETE:
      default:
        return RejectionPolicy.ENQUEUE;
    }
  }
}
