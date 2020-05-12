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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.iotdb.db.trigger.sinks.definition.TriggerSink;
import org.apache.iotdb.db.trigger.sinks.definition.TriggerSinkEvent;
import org.apache.iotdb.db.trigger.sinks.definition.TriggerSinkParameterConfigurations;
import org.simplejavamail.api.email.EmailPopulatingBuilder;
import org.simplejavamail.api.mailer.Mailer;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.mailer.MailerBuilder;

public class MailSink extends TriggerSink {

  private final Mailer mailer;
  private final ConcurrentMap<String, MailTopic> topics;

  public MailSink(TriggerSinkParameterConfigurations configurations) {
    super(configurations);
    MailParameterConfigurations mailParameterConfigurations = (MailParameterConfigurations) configurations;
    mailer = MailerBuilder
        .withTransportStrategy(mailParameterConfigurations.getStrategy())
        .withSMTPServer(
            mailParameterConfigurations.getSmtpHost(), mailParameterConfigurations.getSmtpPort(),
            mailParameterConfigurations.getUsername(), mailParameterConfigurations.getPassword())
        .withProperty("mail.smtp.sendpartial", true)
        .clearEmailAddressCriteria()
        .buildMailer();
    topics = new ConcurrentHashMap<>();
  }

  @Override
  public void open() {
    mailer.testConnection();
  }

  @Override
  public void close() {
  }

  @Override
  public void onEvent(TriggerSinkEvent event) {
    MailEvent mailEvent = (MailEvent) event;
    MailTopic mailTopic = getTopic(mailEvent.getTopic());
    if (mailTopic != null && !mailTopic.getPermissionToSendTheNextMail()) {
      return;
    }
    EmailPopulatingBuilder email = EmailBuilder
        .startingBlank()
        .withHeader("X-Priority", 5)
        .withSubject(mailEvent.getTitle())
        .withPlainText(mailEvent.getBody())
        .from(mailEvent.getSender());
    for (String receiver : mailEvent.getReceivers()) {
      email.to(receiver);
    }
    mailer.sendMail(email.buildEmail());
  }

  public void createTopic(String topic, long minSendingIntervalInMillis,
      Integer maxNumberOfMailSent) {
    MailTopic mailTopic = getTopic(topic);
    if (mailTopic != null) {
      return;
    }
    topics.put(topic, new MailTopic(topic, minSendingIntervalInMillis, maxNumberOfMailSent));
  }

  public void resetTopic(String topic) {
    MailTopic mailTopic = getTopic(topic);
    if (mailTopic == null) {
      return;
    }
    mailTopic.resetNumberOfMailSent();
  }

  private MailTopic getTopic(String topic) {
    return topic == null ? null : topics.get(topic);
  }
}
