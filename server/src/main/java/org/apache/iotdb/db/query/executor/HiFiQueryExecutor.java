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

package org.apache.iotdb.db.query.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.HiFiQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.HiFiQueryDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class HiFiQueryExecutor {

  protected List<Path> deduplicatedPaths;
  protected List<TSDataType> deduplicatedDataTypes;
  protected IExpression optimizedExpression;

  public HiFiQueryExecutor(HiFiQueryPlan queryPlan) {
    this.deduplicatedPaths = queryPlan.getDeduplicatedPaths();
    this.deduplicatedDataTypes = queryPlan.getDeduplicatedDataTypes();
    this.optimizedExpression = queryPlan.getExpression();
  }

  /**
   * execute query without filter or with global time filter.
   */
  public QueryDataSet executeWithoutValueFilter(QueryContext context, HiFiQueryPlan queryPlan)
      throws StorageEngineException, QueryProcessException {
    try {
      return new HiFiQueryDataSetWithoutValueFilter(queryPlan,
          initManagedSeriesReader(context, queryPlan));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageEngineException(e.getMessage());
    } catch (IOException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  protected List<ManagedSeriesReader> initManagedSeriesReader(QueryContext context,
      HiFiQueryPlan queryPlan) throws StorageEngineException, QueryProcessException {
    Filter timeFilter = null;
    if (optimizedExpression != null) {
      timeFilter = ((GlobalTimeExpression) optimizedExpression).getFilter();
    }

    List<ManagedSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    for (int i = 0; i < deduplicatedPaths.size(); i++) {
      Path path = deduplicatedPaths.get(i);
      TSDataType dataType = deduplicatedDataTypes.get(i);

      QueryDataSource queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSource(path, context, timeFilter);
      timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

      ManagedSeriesReader reader = new SeriesRawDataBatchReader(path,
          queryPlan.getAllMeasurementsInDevice(path.getDevice()), dataType, context,
          queryDataSource, timeFilter, null, null);
      readersOfSelectedSeries.add(reader);
    }
    return readersOfSelectedSeries;
  }

  public QueryDataSet executeWithValueFilter(QueryContext context, HiFiQueryPlan queryPlan) {
    return null;
  }
}
