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

package org.apache.iotdb.db.query.hifi.weight;

public class TriangularAreaOperator<T extends Number & Comparable<? super T>> extends
    WeightOperator<T> {

  @Override
  public Double operator(Long time0, Long time1, Long time2, T value0, T value1, T value2) {
    double v0 = value0.doubleValue();
    double v1 = value1.doubleValue();
    double v2 = value2.doubleValue();
    return 0.5 * (time0 * v1 + time1 * v2 + time2 * v0 - time0 * v2 - time1 * v0 - time2 * v1);
  }
}
