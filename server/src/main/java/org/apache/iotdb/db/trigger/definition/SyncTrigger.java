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

public abstract class SyncTrigger extends Trigger {

  public SyncTrigger(String path, String id, int enabledHooks,
      TriggerParameterConfigurations parameters, boolean isActive) {
    super(path, id, enabledHooks, parameters, isActive);
  }

  @Override
  public final boolean isSynced() {
    return true;
  }

  /**
   * This hook will be called if {@link Trigger#conditionBeforeInsertRecord(long, Object)} returns
   * {@code true}.
   *
   * @param timestamp The timestamp of the data point to be inserted.
   * @param value The value of the data point to be inserted.
   * @return The actual value to be inserted. Return {@code null} to discard the whole record (all
   * data points which have the same timestamp in the insert operation). Please make sure the return
   * value has the same type as the param {@code value}.
   */
  public Object actionBeforeInsertRecord(final long timestamp, final Object value) {
    return value;
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
   * returns {@code true}. Modify {@code timestamps} in this method may cause undefined behavior.
   *
   * @param timestamps All timestamps in the tablet to be inserted.
   * @param values All values in the tablet to be inserted.
   * @return The actual values to be inserted. Return {@code null} to terminate the tablet insert.
   * Please make sure the return value has the same type as the param {@code values}. The length of
   * the return array should always be the same as the length of {@code timestamps}.
   */
  public Object actionBeforeInsertTablet(final long[] timestamps, final Object values) {
    return values;
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
   * @return The actual timestamp in the delete operation. Return {@code null} to terminate the
   * delete operation.
   */
  public Long actionBeforeDelete(final long timestamp) {
    return timestamp;
  }

  /**
   * This hook will be called if {@link Trigger#conditionAfterDelete(long)} returns {@code true}.
   *
   * @param timestamp The timestamp in the delete operation.
   */
  public void actionAfterDelete(final long timestamp) {
  }
}
