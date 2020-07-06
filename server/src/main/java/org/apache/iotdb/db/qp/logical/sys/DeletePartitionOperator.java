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

package org.apache.iotdb.db.qp.logical.sys;

import java.util.Set;
import org.apache.iotdb.db.qp.logical.RootOperator;

public class DeletePartitionOperator extends RootOperator {

  private String storageGroupName;
  private Set<Long> partitionIds;

  public DeletePartitionOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.DELETE_PARTITION;
  }

  public void setStorageGroupName(String storageGroupName) {
    this.storageGroupName = storageGroupName;
  }

  public void setPartitionIds(Set<Long> partitionIds) {
    this.partitionIds = partitionIds;
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public Set<Long> getPartitionId() {
    return partitionIds;
  }
}
