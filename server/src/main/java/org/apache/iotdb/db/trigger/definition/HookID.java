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

import java.util.ArrayList;
import java.util.List;

public enum HookID {

  BEFORE_INSERT(0B00000001),
  BEFORE_DELETE(0B00000010),
  //  BEFORE_UPDATE(0B00000100),
  BEFORE_BATCH_INSERT(0B00001000),

  AFTER_INSERT(0B00010000),
  AFTER_DELETE(0B00100000),
  //  AFTER_UPDATE(0B01000000),
  AFTER_BATCH_INSERT(0B10000000),

  ON_ALL_EVENTS(~0B0),
  ;

  private final int id;

  HookID(int id) {
    this.id = id;
  }

  /**
   * Test if the enabled hooks contains the hook marked by {@link HookID#id}. Due to performance
   * considerations, it cannot be used for testing {@link HookID#ON_ALL_EVENTS}.
   *
   * @param enabledHooks the enabled hooks
   * @return whether the hook is enabled
   */
  public boolean isEnabled(int enabledHooks) {
    return 0 < (id & enabledHooks);
  }

  public int getId() {
    return id;
  }

  public static String show(int enabledHooks) {
    List<String> enabledHookNames = new ArrayList<>();
    for (HookID hookID : values()) {
      if (hookID.isEnabled(enabledHooks)) {
        enabledHookNames.add(hookID.toString());
      }
    }
    enabledHookNames.remove(ON_ALL_EVENTS.toString());
    return enabledHookNames.size() == values().length - 1 ? ON_ALL_EVENTS.toString()
        : String.join(" & ", enabledHookNames);
  }

  @Override
  public String toString() {
    switch (this) {
      case BEFORE_INSERT:
        return "before insert";
      case AFTER_INSERT:
        return "after insert";
      case BEFORE_DELETE:
        return "before delete";
      case AFTER_DELETE:
        return "after delete";
//      case BEFORE_UPDATE:
//        return "before update";
//      case AFTER_UPDATE:
//        return "after update";
      case BEFORE_BATCH_INSERT:
        return "before batch insert";
      case AFTER_BATCH_INSERT:
        return "after batch insert";
      case ON_ALL_EVENTS:
        return "all events";
      default:
        throw new UnsupportedOperationException("Unsupported HookID.");
    }
  }
}
