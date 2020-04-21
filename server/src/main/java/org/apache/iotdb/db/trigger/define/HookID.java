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

package org.apache.iotdb.db.trigger.define;

public enum HookID {

  ON_DATA_POINT_BEFORE_INSERT(0B00000001),
  ON_DATA_POINT_BEFORE_DELETE(0B00000010),
  ON_DATA_POINT_BEFORE_UPDATE(0B00000100),

  ON_DATA_POINT_AFTER_INSERT (0B00001000),
  ON_DATA_POINT_AFTER_DELETE (0B00010000),
  ON_DATA_POINT_AFTER_UPDATE (0B00100000),

  ON_BATCH_BEFORE_INSERT     (0B00010000),
  ON_BATCH_AFTER_INSERT      (0B00100000),
  ;

  private final int id;

  HookID(int id) {
    this.id = id;
  }

  public boolean isEnabled(int enableHooks) {
    return 0 < (id & enableHooks);
  }
}
