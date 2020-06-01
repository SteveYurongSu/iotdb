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

package org.apache.iotdb.trigger.sinks.mail;

import org.apache.iotdb.db.trigger.sink.TriggerSinkParameterConfigurations;
import org.simplejavamail.api.mailer.config.TransportStrategy;

public class MailParameterConfigurations extends TriggerSinkParameterConfigurations {

  private final TransportStrategy strategy;
  private final String smtpHost;
  private final Integer smtpPort;
  private final String username;
  private final String password;

  public MailParameterConfigurations(String className, String id, String transportStrategy,
      String smtpHost, Integer smtpPort, String username, String password) {
    super(className, id);
    transportStrategy = transportStrategy.toUpperCase();
    switch (transportStrategy) {
      case "SMTP":
        strategy = TransportStrategy.SMTP;
        break;
      case "SMTPS":
        strategy = TransportStrategy.SMTPS;
        break;
      case "SMTP_TLS":
        strategy = TransportStrategy.SMTP_TLS;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported SMTP Transport Strategy.");
    }
    this.smtpHost = smtpHost;
    this.smtpPort = smtpPort;
    this.username = username;
    this.password = password;
  }

  public TransportStrategy getStrategy() {
    return strategy;
  }

  public String getSmtpHost() {
    return smtpHost;
  }

  public Integer getSmtpPort() {
    return smtpPort;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
