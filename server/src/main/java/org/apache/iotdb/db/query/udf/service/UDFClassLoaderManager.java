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

package org.apache.iotdb.db.query.udf.service;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDFClassLoaderManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(UDFClassLoaderManager.class);

  private final String libRoot;

  /**
   * The keys in the map are the query IDs of the UDF queries being executed.
   */
  private final Map<Long, UDFClassLoader> queryIdToUDFClassLoaderMap;

  /**
   * activeClassLoader is used to load all classes under libRoot. libRoot may be updated before the
   * user executes CREATE FUNCTION or after the user executes DROP FUNCTION. Therefore, we need to
   * continuously maintain the activeClassLoader so that the classes it loads are always up to
   * date.
   */
  @SuppressWarnings("squid:S3077")
  private volatile UDFClassLoader activeClassLoader;

  UDFClassLoaderManager() {
    libRoot = parseLibRoot();
    queryIdToUDFClassLoaderMap = new ConcurrentHashMap<>();
    activeClassLoader = null;
  }

  private String parseLibRoot() {
    String jarPath = (new File(
        getClass().getProtectionDomain().getCodeSource().getLocation().getPath()))
        .getAbsolutePath();
    int lastIndex = jarPath.lastIndexOf(File.separatorChar);
    String libPath = jarPath.substring(0, lastIndex + 1);
    logger.info("System lib root: {}", libPath);
    return libPath;
  }

  public void initializeUDFQuery(long queryId) {
    activeClassLoader.acquire();
    queryIdToUDFClassLoaderMap.put(queryId, activeClassLoader);
  }

  public void finalizeUDFQuery(long queryId) throws IOException {
    UDFClassLoader classLoader = queryIdToUDFClassLoaderMap.remove(queryId);
    classLoader.release();
  }

  public UDFClassLoader updateAndGetActiveClassLoader() throws IOException {
    UDFClassLoader deprecatedClassLoader = activeClassLoader;
    activeClassLoader = new UDFClassLoader(libRoot);
    deprecatedClassLoader.markAsDeprecated();
    return activeClassLoader;
  }

  public UDFClassLoader getActiveClassLoader() {
    return activeClassLoader;
  }

  @Override
  public void start() throws StartupException {
    try {
      activeClassLoader = new UDFClassLoader(libRoot);
    } catch (IOException e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    // nothing to do
  }

  @Override
  public ServiceType getID() {
    return ServiceType.UDF_CLASSLOADER_MANAGER_SERVICE;
  }

  public static UDFClassLoaderManager getInstance() {
    return UDFClassLoaderManager.UDFClassLoaderManagerHelper.INSTANCE;
  }

  private static class UDFClassLoaderManagerHelper {

    private static final UDFClassLoaderManager INSTANCE = new UDFClassLoaderManager();

    private UDFClassLoaderManagerHelper() {
    }
  }
}
