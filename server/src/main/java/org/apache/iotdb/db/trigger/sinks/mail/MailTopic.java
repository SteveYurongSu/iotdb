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

package org.apache.iotdb.db.trigger.sinks.mail;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class MailTopic {

  private final String topic;
  private final long minSendingIntervalInMillis;
  private final Integer maxNumberOfMailSent;
  private int numberOfMailSent;
  private LocalDateTime lastTimeOfEmailSent;

  public MailTopic(String topic, long minSendingIntervalInMillis, Integer maxNumberOfMailSent) {
    this.topic = topic;
    this.minSendingIntervalInMillis = minSendingIntervalInMillis;
    this.maxNumberOfMailSent = maxNumberOfMailSent;
    numberOfMailSent = 0;
    lastTimeOfEmailSent = null;
  }

  public boolean getPermissionToSendTheNextMail() {
    synchronized (this) {
      if ((maxNumberOfMailSent == null || numberOfMailSent < maxNumberOfMailSent)
          && (lastTimeOfEmailSent == null || lastTimeOfEmailSent
          .plus(minSendingIntervalInMillis, ChronoUnit.MILLIS).isBefore(LocalDateTime.now()))) {
        recordMailSent();
        return true;
      }
      return false;
    }
  }

  public void resetNumberOfMailSent() {
    synchronized (this) {
      numberOfMailSent = 0;
    }
  }

  private void recordMailSent() {
    lastTimeOfEmailSent = LocalDateTime.now();
    ++numberOfMailSent;
  }

  public String getTopic() {
    return topic;
  }
}
