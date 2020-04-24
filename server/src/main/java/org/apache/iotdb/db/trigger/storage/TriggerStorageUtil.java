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

import static org.apache.iotdb.db.trigger.storage.TriggerStorageConstant.TRIGGER_BASE_DIRECTORY;
import static org.apache.iotdb.db.trigger.storage.TriggerStorageConstant.TRIGGER_CONFIGURATION_FILENAME;
import static org.apache.iotdb.db.trigger.storage.TriggerStorageConstant.TRIGGER_INSTANCE_DIRECTORY;
import static org.apache.iotdb.db.trigger.storage.TriggerStorageConstant.TRIGGER_INSTANCE_FILENAME_EXTENSION;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.trigger.TriggerInstanceLoadException;
import org.apache.iotdb.db.exception.trigger.TriggerManagementException;
import org.apache.iotdb.db.trigger.define.Trigger;
import org.apache.iotdb.db.trigger.define.TriggerParameterConfiguration;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerStorageUtil.class);

  private TriggerStorageUtil() {
  }

  public static boolean makeTriggerConfigurationFileIfNecessary() {
    boolean result = true;
    File file = new File(TRIGGER_CONFIGURATION_FILENAME);
    if (!file.exists()) {
      XMLWriter writer = null;
      try {
        Document document = DocumentHelper.createDocument();
        document.addElement("triggers");
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");
        writer = new XMLWriter(new FileWriter(TRIGGER_CONFIGURATION_FILENAME), format);
        writer.write(document);
        writer.flush();
      } catch (IOException e) {
        LOGGER.info("Failed to create file {}, because {}", TRIGGER_CONFIGURATION_FILENAME,
            e.getMessage());
        result = false;
      } finally {
        try {
          if (writer != null) {
            writer.close();
          }
        } catch (IOException ignored) {
        }
      }
    } else {
      LOGGER.info("File {} already exists.", TRIGGER_CONFIGURATION_FILENAME);
    }
    return result;
  }

  public static boolean makeTriggerStorageDirectoriesIfNecessary() {
    String[] checkList = {
        TRIGGER_BASE_DIRECTORY,
        TRIGGER_INSTANCE_DIRECTORY,
    };
    boolean isSuccessful = true;
    for (String dir : checkList) {
      File file = new File(dir);
      if (!file.exists()) {
        boolean result = file.mkdirs();
        isSuccessful = isSuccessful && result;
        LOGGER.info("Make directory {} {}.", dir, result ? "successfully" : "unsuccessfully");
      } else {
        LOGGER.info("Directory {} already exists.", dir);
      }
    }
    return isSuccessful;
  }

  public static List<Trigger> recoveryTriggersFromConfigurationFile()
      throws TriggerInstanceLoadException {
    List<Trigger> triggers = new ArrayList<>();
    SAXReader reader = new SAXReader();
    try {
      Document document = reader.read(new File(TRIGGER_CONFIGURATION_FILENAME));
      Element root = document.getRootElement();
      for (Element element : root.elements()) {
        TriggerParameterConfiguration[] parameterConfigurations = parseTriggerParameterConfigurationFromHookElement(
            element);
        int enabledHooks = Integer.parseInt(element.attribute("enabledHooks").getText());
        boolean isActive = !"false".equals(element.attribute("isActive").getText());
        triggers.add(createTriggerInstanceFromJar(element.attribute("class").getText(),
            element.attribute("path").getText(), element.attribute("id").getText(),
            enabledHooks, parameterConfigurations, isActive));
      }
    } catch (DocumentException e) {
      throw new TriggerInstanceLoadException(String
          .format("Failed to read trigger configuration file: %s, because %s",
              TRIGGER_CONFIGURATION_FILENAME, e.getMessage()));
    }
    return triggers;
  }

  public static void registerTriggerToConfigurationFile(Trigger trigger)
      throws TriggerManagementException {
    SAXReader reader = new SAXReader();
    XMLWriter writer = null;
    try {
      Document document = reader.read(new File(TRIGGER_CONFIGURATION_FILENAME));
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
      if (0 < trigger.getParameters().length) {
        Element parametersElement = element.addElement("parameters");
        for (TriggerParameterConfiguration parameterConfiguration : trigger.getParameters()) {
          Element parameterElement = parametersElement.addElement("parameter");
          parameterElement.addAttribute("name", parameterConfiguration.getName());
          parameterElement.addAttribute("value", parameterConfiguration.getValue());
        }
      }

      OutputFormat format = OutputFormat.createPrettyPrint();
      format.setEncoding("utf-8");
      writer = new XMLWriter(new FileWriter(TRIGGER_CONFIGURATION_FILENAME), format);
      writer.write(document);
      writer.flush();
    } catch (DocumentException | IOException e) {
      throw new TriggerManagementException(
          String.format("Failed to register %s to file: %s, because %s",
              trigger.toString(), TRIGGER_CONFIGURATION_FILENAME, e.getMessage()));
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
    SAXReader reader = new SAXReader();
    XMLWriter writer = null;
    try {
      Document document = reader.read(new File(TRIGGER_CONFIGURATION_FILENAME));
      Element root = document.getRootElement();
      if (!doTriggerRemovalInElements(trigger, root.elements())) {
        return;
      }

      OutputFormat format = OutputFormat.createPrettyPrint();
      format.setEncoding("utf-8");
      writer = new XMLWriter(new FileWriter(TRIGGER_CONFIGURATION_FILENAME), format);
      writer.write(document);
      writer.flush();
    } catch (DocumentException | IOException e) {
      throw new TriggerManagementException(
          String.format("Failed to remove %s from file: %s, because %s",
              trigger.toString(), TRIGGER_CONFIGURATION_FILENAME, e.getMessage()));
    } finally {
      try {
        if (writer != null) {
          writer.close();
        }
      } catch (IOException ignored) {
      }
    }
  }

  public static Trigger createTriggerInstanceFromJar(Trigger trigger)
      throws TriggerInstanceLoadException {
    return createTriggerInstanceFromJar(trigger.getClass().getName(), trigger.getPath(),
        trigger.getId(), trigger.getEnabledHooks(), trigger.getParameters(), trigger.isActive());
  }

  public static Trigger createTriggerInstanceFromJar(String className, String path,
      String id, int enabledHooks, TriggerParameterConfiguration[] parameterConfigurations,
      boolean isActive) throws TriggerInstanceLoadException {
    try {
      URL url = new URL(String
          .format("file:%s", getTriggerInstanceJarFilepathByClassName(className)));
      URLClassLoader classLoader = new URLClassLoader(new URL[]{url}, Thread.currentThread()
          .getContextClassLoader());
      Class<?> triggerClass = classLoader.loadClass(className);
      Constructor<?> constructor = triggerClass.getConstructor(String.class, String.class,
          int.class, TriggerParameterConfiguration[].class, boolean.class);
      return (Trigger) constructor.newInstance(path, id, enabledHooks, parameterConfigurations,
          isActive);
    } catch (ClassNotFoundException | MalformedURLException | IllegalAccessException
        | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
      throw new TriggerInstanceLoadException(String.format(
          "Failed to load Trigger(Path: %s, ID: %s, IsActive: %s, ClassName: %s) from file: %s, because %s",
          path, id, isActive ? "true" : "false", className,
          getTriggerInstanceJarFilepathByClassName(className), e.getMessage()));
    }
  }

  private static String getTriggerInstanceJarFilepathByClassName(String className) {
    return TRIGGER_INSTANCE_DIRECTORY + File.separator + className
        + TRIGGER_INSTANCE_FILENAME_EXTENSION;
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

  private static TriggerParameterConfiguration[] parseTriggerParameterConfigurationFromHookElement(
      Element hook) {
    TriggerParameterConfiguration[] parameterConfigurations;
    Element parametersElement = hook.element("parameters");
    if (parametersElement != null) {
      List<Element> parameterElementList = parametersElement.elements("parameter");
      parameterConfigurations = new TriggerParameterConfiguration[parameterElementList.size()];
      for (int i = 0; i < parameterElementList.size(); ++i) {
        parameterConfigurations[i] = new TriggerParameterConfiguration(
            parameterElementList.get(i).attribute("name").getText(),
            parameterElementList.get(i).attribute("value").getText());
      }
    } else {
      parameterConfigurations = new TriggerParameterConfiguration[0];
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
