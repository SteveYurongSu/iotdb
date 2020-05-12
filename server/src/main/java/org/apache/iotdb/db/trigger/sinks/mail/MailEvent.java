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

import org.apache.iotdb.db.trigger.sinks.definition.TriggerSinkEvent;

public class MailEvent implements TriggerSinkEvent {

  private final String topic;
  private final String title;
  private final String body;
  private final String sender;
  private final String[] receivers;

  public MailEvent(String topic, String title, String body, String sender, String... receivers) {
    this.topic = topic;
    this.title = title;
    this.body = body;
    this.sender = sender;
    this.receivers = receivers;
  }

  public String getTopic() {
    return topic;
  }

  public String getTitle() {
    return title;
  }

  public String getBody() {
    return body;
  }

  public String getSender() {
    return sender;
  }

  public String[] getReceivers() {
    return receivers;
  }
}
