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

package org.apache.iotdb.db.trigger.storage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.trigger.TriggerInstanceLoadException;
import org.apache.iotdb.db.exception.trigger.TriggerManagementException;
import org.apache.iotdb.db.trigger.definition.Trigger;
import org.apache.iotdb.db.trigger.definition.TriggerParameterConfigurations;
import org.apache.iotdb.db.utils.TestOnly;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerStorageUtil {

  private static final Logger logger = LoggerFactory.getLogger(TriggerStorageUtil.class);

  private TriggerStorageUtil() {
  }

  public static void makeTriggerDirectoryIfNecessary() throws IOException {
    String triggerDir = IoTDBDescriptor.getInstance().getConfig().getTriggerDir();
    File triggerFolder = SystemFileFactory.INSTANCE.getFile(triggerDir);
    if (!triggerFolder.exists()) {
      if (triggerFolder.mkdirs()) {
        logger.info("Folder {} was created successfully.", triggerFolder.getAbsolutePath());
      } else {
        logger.error("Failed to create folder {}.", triggerFolder.getAbsolutePath());
        throw new IOException(String.format("Failed to create folder %s.",
            triggerFolder.getAbsolutePath()));
      }
    }
  }

  public static void makeTriggerConfigurationFileIfNecessary() throws IOException {
    String filename = IoTDBDescriptor.getInstance().getConfig().getTriggerConfigurationFilename();
    File file = new File(filename);
    if (!file.exists()) {
      XMLWriter writer = null;
      try {
        Document document = DocumentHelper.createDocument();
        document.addElement("triggers");
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");
        writer = new XMLWriter(new FileWriter(filename), format);
        writer.write(document);
        writer.flush();
        logger.info("File {} was created successfully.", filename);
      } finally {
        try {
          if (writer != null) {
            writer.close();
          }
        } catch (IOException ignored) {
        }
      }
    } else {
      logger.info("File {} already exists.", filename);
    }
  }

  public static List<Trigger> recoveryTriggersFromConfigurationFile()
      throws TriggerInstanceLoadException {
    String filename = IoTDBDescriptor.getInstance().getConfig().getTriggerConfigurationFilename();
    List<Trigger> triggers = new ArrayList<>();
    SAXReader reader = new SAXReader();
    try {
      Document document = reader.read(new File(filename));
      Element root = document.getRootElement();
      for (Element element : root.elements()) {
        TriggerParameterConfigurations parameterConfigurations = parseTriggerParameterConfigurationFromHookElement(
            element);
        int enabledHooks = Integer.parseInt(element.attribute("enabledHooks").getText());
        boolean isActive = !"false".equals(element.attribute("isActive").getText());
        triggers.add(createTriggerInstance(element.attribute("class").getText(),
            element.attribute("path").getText(), element.attribute("id").getText(),
            enabledHooks, parameterConfigurations, isActive));
      }
    } catch (DocumentException e) {
      throw new TriggerInstanceLoadException(String
          .format("Failed to read trigger configuration file: %s, because %s", filename,
              e.getMessage()));
    }
    return triggers;
  }

  public static void registerTriggerToConfigurationFile(Trigger trigger)
      throws TriggerManagementException {
    String filename = IoTDBDescriptor.getInstance().getConfig().getTriggerConfigurationFilename();
    SAXReader reader = new SAXReader();
    XMLWriter writer = null;
    try {
      Document document = reader.read(new File(filename));
      Element root = document.getRootElement();
      if (triggerWithTheSameIdOrSyncTypeHasAlreadyBeenRegistered(trigger, root.elements())) {
        throw new TriggerManagementException(
            String.format("Failed to register %s to file, because %s", trigger.toString(),
                "a trigger with the same id or sync type has already been registered."));
      }

      Element element = root.addElement("trigger");
      element.addAttribute("class", trigger.getClass().getTypeName());
      element.addAttribute("path", trigger.getPath());
      element.addAttribute("id", trigger.getId());
      element.addAttribute("enabledHooks", String.valueOf(trigger.getEnabledHooks()));
      element.addAttribute("isSynced", trigger.isSynced() ? "true" : "false");
      element.addAttribute("isActive", trigger.isActive() ? "true" : "false");
      Set<Entry<String, String>> configurations = trigger.getParameters()
          .getConfigurationEntrySet();
      if (0 < configurations.size()) {
        Element parametersElement = element.addElement("parameters");
        for (Entry<String, String> configuration : configurations) {
          Element parameterElement = parametersElement.addElement("parameter");
          parameterElement.addAttribute("name", configuration.getKey());
          parameterElement.addAttribute("value", configuration.getValue());
        }
      }

      OutputFormat format = OutputFormat.createPrettyPrint();
      format.setEncoding("utf-8");
      writer = new XMLWriter(new FileWriter(filename), format);
      writer.write(document);
      writer.flush();
    } catch (DocumentException | IOException e) {
      throw new TriggerManagementException(String
          .format("Failed to register %s to file: %s, because %s", trigger.toString(), filename,
              e.getMessage()));
    } finally {
      try {
        if (writer != null) {
          writer.close();
        }
      } catch (IOException ignored) {
      }
    }
  }

  public static void removeTriggerFromConfigurationFile(Trigger trigger)
      throws TriggerManagementException {
    String filename = IoTDBDescriptor.getInstance().getConfig().getTriggerConfigurationFilename();
    SAXReader reader = new SAXReader();
    XMLWriter writer = null;
    try {
      Document document = reader.read(new File(filename));
      Element root = document.getRootElement();
      if (!doTriggerRemovalInElements(trigger, root.elements())) {
        return;
      }

      OutputFormat format = OutputFormat.createPrettyPrint();
      format.setEncoding("utf-8");
      writer = new XMLWriter(new FileWriter(filename), format);
      writer.write(document);
      writer.flush();
    } catch (DocumentException | IOException e) {
      throw new TriggerManagementException(String
          .format("Failed to remove %s from file: %s, because %s", trigger.toString(), filename,
              e.getMessage()));
    } finally {
      try {
        if (writer != null) {
          writer.close();
        }
      } catch (IOException ignored) {
      }
    }
  }

  public static Trigger createTriggerInstance(Trigger trigger) throws TriggerInstanceLoadException {
    return createTriggerInstance(trigger.getClass().getName(), trigger.getPath(), trigger.getId(),
        trigger.getEnabledHooks(), trigger.getParameters(), trigger.isActive());
  }

  public static Trigger createTriggerInstance(String className, String path, String id,
      int enabledHooks, TriggerParameterConfigurations parameterConfigurations, boolean isActive)
      throws TriggerInstanceLoadException {
    try {
      Class<?> triggerClass = Class.forName(className, true, Thread.currentThread()
          .getContextClassLoader());
      Constructor<?> constructor = triggerClass.getConstructor(String.class, String.class,
          int.class, TriggerParameterConfigurations.class, boolean.class);
      return (Trigger) constructor.newInstance(path, id, enabledHooks, parameterConfigurations,
          isActive);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException
        | InvocationTargetException | NoSuchMethodException e) {
      throw new TriggerInstanceLoadException(String.format(
          "Failed to load Trigger(Path: %s, ID: %s, IsActive: %s, ClassName: %s), because %s",
          path, id, isActive ? "true" : "false", className, e.getMessage()));
    }
  }

  private static boolean triggerWithTheSameIdOrSyncTypeHasAlreadyBeenRegistered(Trigger trigger,
      List<Element> elements) {
    for (Element e : elements) {
      if (e.attribute("id").getText().equals(trigger.getId())
          || (e.attribute("isSynced").getText().equals(trigger.isSynced() ? "true" : "false")
          && e.attribute("path").getText().equals(trigger.getPath()))) {
        return true;
      }
    }
    return false;
  }

  private static boolean doTriggerRemovalInElements(Trigger trigger, List<Element> elements) {
    for (Element e : elements) {
      if (e.attribute("id").getText().equals(trigger.getId())) {
        e.getParent().remove(e);
        return true;
      }
    }
    return false;
  }

  private static TriggerParameterConfigurations parseTriggerParameterConfigurationFromHookElement(
      Element hook) {
    TriggerParameterConfigurations parameterConfigurations = new TriggerParameterConfigurations();
    Element parametersElement = hook.element("parameters");
    if (parametersElement != null) {
      List<Element> parameterElementList = parametersElement.elements("parameter");
      for (Element element : parameterElementList) {
        parameterConfigurations
            .put(element.attribute("name").getText(), element.attribute("value").getText());
      }
    }
    return parameterConfigurations;
  }

  @TestOnly
  public static void deleteFile(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      assert files != null;
      for (File f : files) {
        deleteFile(f);
      }
    }
    file.delete();
  }
}
