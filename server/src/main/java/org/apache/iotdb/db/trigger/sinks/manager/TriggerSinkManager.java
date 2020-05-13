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

package org.apache.iotdb.db.trigger.sinks.manager;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.iotdb.db.exception.trigger.TriggerSinkLoadException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.trigger.sinks.definition.TriggerSink;
import org.apache.iotdb.db.trigger.sinks.definition.TriggerSinkParameterConfigurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerSinkManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(TriggerSinkManager.class);

  private final ConcurrentMap<String, TriggerSink> idToSinks;

  private TriggerSinkManager() {
    idToSinks = new ConcurrentHashMap<>();
  }

  @Override
  public void start() {
    logger.info("TriggerSinkManager service started.");
  }

  @Override
  public void stop() {
    closeSinks();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TRIGGER_SINK_SERVICE;
  }

  public TriggerSink open(TriggerSinkParameterConfigurations configurations)
      throws TriggerSinkLoadException {
    String id = configurations.getId();
    TriggerSink sink = idToSinks.get(id);
    if (sink == null) {
      sink = createSinkInstance(configurations);
      sink.open();
      idToSinks.put(id, sink);
    }
    sink.increaseReferenceCount();
    return sink;
  }

  public void close(String id) {
    TriggerSink sink = idToSinks.get(id);
    if (sink == null) {
      return;
    }
    sink.decreaseReferenceCount();
    if (sink.getReferenceCount() == 0) {
      sink.close();
      idToSinks.remove(id);
    }
  }

  private TriggerSink createSinkInstance(TriggerSinkParameterConfigurations configurations)
      throws TriggerSinkLoadException {
    try {
      Class<?> sinkClass = Class.forName(configurations.getClassName(), true,
          Thread.currentThread().getContextClassLoader());
      Constructor<?> constructor = sinkClass
          .getConstructor(TriggerSinkParameterConfigurations.class);
      return (TriggerSink) constructor.newInstance(configurations);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException
        | InvocationTargetException | NoSuchMethodException e) {
      e.printStackTrace();
      throw new TriggerSinkLoadException(String
          .format("Failed to load TriggerSink(ClassName: %s), because %s",
              configurations.getClassName(), e.getMessage()));
    }
  }

  private void closeSinks() {
    for (String id : idToSinks.keySet()) {
      TriggerSink sink = idToSinks.remove(id);
      sink.close();
    }
  }

  public static TriggerSinkManager getInstance() {
    return TriggerSinkManager.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static final TriggerSinkManager INSTANCE = new TriggerSinkManager();
  }
}
