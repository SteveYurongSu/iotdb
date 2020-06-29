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

public class CosineOperator<T extends Number & Comparable<? super T>> extends WeightOperator<T> {

  @Override
  public Double operator(Long time0, Long time1, Long time2, T value0, T value1, T value2) {
    double x0 = time1 - time0;
    double x1 = time2 - time1;
    double y0 = value1.doubleValue() - value0.doubleValue();
    double y1 = value2.doubleValue() - value1.doubleValue();
    double distance0 = Math.sqrt(x0 * x0 + y0 * y0);
    double distance1 = Math.sqrt(x1 * x1 + y1 * y1);
    return 1 - ((x0 * x1 + y0 * y1) / (distance0 * distance1));
  }
}
