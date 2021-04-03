package org.apache.iotdb.db.cq;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContinuousQuery implements Runnable {

  private PlanExecutor planExecutor;
  private CreateContinuousQueryPlan plan;
  private Planner planner;

  public ContinuousQuery(CreateContinuousQueryPlan plan) throws QueryProcessException {
    this.plan = plan;
    this.planExecutor = new PlanExecutor();
    this.planner = new Planner();
  }

  private static TSDataType getAggrDataType(String aggrFuncName, TSDataType dataType) {
    if (aggrFuncName == null) {
      throw new IllegalArgumentException("AggregateFunction Name must not be null");
    }

    switch (aggrFuncName.toLowerCase()) {
      case SQLConstant.MIN_TIME:
      case SQLConstant.MAX_TIME:
      case SQLConstant.COUNT:
        return TSDataType.INT64;
      case SQLConstant.MIN_VALUE:
      case SQLConstant.LAST_VALUE:
      case SQLConstant.FIRST_VALUE:
      case SQLConstant.MAX_VALUE:
        return dataType;
      case SQLConstant.AVG:
      case SQLConstant.SUM:
        return TSDataType.DOUBLE;
      default:
        throw new IllegalArgumentException("Invalid Aggregation function: " + aggrFuncName);
    }
  }

  @Override
  public void run() {

    System.out.println("===============schedule===============");

    GroupByTimePlan queryPlan = null;

    try {

      queryPlan = (GroupByTimePlan) planner.parseSQLToPhysicalPlan(plan.getQuerySql());

      // plan.getQueryOperator().getSelectOperator().getSuffixPaths();

      //      queryPlan =
      //          (GroupByTimePlan) planner.queryOperatorToPhysicalPlan(plan.getQueryOperator(),
      // 1024);

      long timestamp = System.currentTimeMillis();
      queryPlan.setStartTime(timestamp - plan.getForInterval());
      queryPlan.setEndTime(timestamp);

    } catch (QueryProcessException e) {
      e.printStackTrace();
    }

    long queryId =
        QueryResourceManager.getInstance()
            .assignQueryId(true, 1024, queryPlan.getDeduplicatedPaths().size());

    QueryDataSet result = null;
    try {
      result = planExecutor.processQuery(queryPlan, new QueryContext(queryId));
    } catch (Exception e) {
      e.printStackTrace();
    }

    if (result == null) {
      return;
    }

    try {
      QueryResourceManager.getInstance().endQuery(queryId);
    } catch (StorageEngineException e) {
      e.printStackTrace();
    }

    List<PartialPath> targetPaths = null;
    try {
      targetPaths = getTargetPaths(result.getPaths());
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }

    int columnSize = result.getDataTypes().size();

    InsertTabletPlan[] insertTabletPlans = new InsertTabletPlan[columnSize];

    String[] measurements = new String[] {targetPaths.get(0).getMeasurement()};
    TSDataType dataType =
        getAggrDataType(queryPlan.getAggregations().get(0), queryPlan.getDataTypes().get(0));
    List<Integer> dataTypes = Collections.singletonList(dataType.ordinal());

    for (int i = 0; i < columnSize; i++) {
      try {
        insertTabletPlans[i] =
            new InsertTabletPlan(
                new PartialPath(targetPaths.get(i).getDevice()), measurements, dataTypes);
      } catch (IllegalPathException e) {
        e.printStackTrace();
      }
    }

    int fetchSize =
        (int) Math.min(10, plan.getForInterval() / plan.getQueryOperator().getUnit() + 1);

    Object[][] columns = new Object[columnSize][1];
    for (int i = 0; i < columnSize; i++) {
      switch (dataType) {
        case DOUBLE:
          columns[i][0] = new double[fetchSize];
          break;
        case INT64:
          columns[i][0] = new long[fetchSize];
          break;
        case INT32:
          columns[i][0] = new int[fetchSize];
          break;
        case FLOAT:
          columns[i][0] = new float[fetchSize];
          break;
      }
    }
    long[][] timestamps = new long[columnSize][fetchSize];
    int[] rowNums = new int[columnSize];

    try {

      while (true) {
        int rowNum = 0;
        for (int i = 0; i < rowNums.length; i++) {
          rowNums[i] = 0;
        }

        boolean hasNext = true;

        while (true) {
          if (++rowNum > fetchSize) {
            break;
          }
          if (!result.hasNextWithoutConstraint()) {
            hasNext = false;
            break;
          }

          RowRecord r = result.nextWithoutConstraint();
          List<Field> fields = r.getFields();
          long ts = r.getTimestamp();

          for (int i = 0; i < columnSize; i++) {
            Field f = fields.get(i);
            if (f != null) {
              timestamps[i][rowNums[i]] = ts;
              switch (dataType) {
                case DOUBLE:
                  ((double[]) columns[i][0])[rowNums[i]] = f.getDoubleV();
                  break;
                case INT64:
                  ((long[]) columns[i][0])[rowNums[i]] = f.getLongV();
                  break;
                case INT32:
                  ((int[]) columns[i][0])[rowNums[i]] = f.getIntV();
                  break;
                case FLOAT:
                  ((float[]) columns[i][0])[rowNums[i]] = f.getFloatV();
                  break;
              }
              rowNums[i]++;
            }
          }
        }

        for (int i = 0; i < columnSize; i++) {
          if (rowNums[i] > 0) {
            insertTabletPlans[i].setTimes(timestamps[i]);
            insertTabletPlans[i].setColumns(columns[i]);
            insertTabletPlans[i].setRowCount(rowNums[i]);
            try {
              planExecutor.insertTablet(insertTabletPlans[i]);
            } catch (QueryProcessException e) {
              e.printStackTrace();
            }
          }
        }

        if (!hasNext) {
          break;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private List<PartialPath> getTargetPaths(List<Path> rawPaths) throws IllegalPathException {
    List<PartialPath> targetPaths = new ArrayList<>(rawPaths.size());
    for (int i = 0; i < rawPaths.size(); i++) {
      targetPaths.add(new PartialPath(fillTemplate((PartialPath) rawPaths.get(i))));
    }
    return targetPaths;
  }

  private String fillTemplate(PartialPath rawPath) {
    String[] nodes = rawPath.getNodes();
    StringBuffer sb = new StringBuffer();
    Matcher m = Pattern.compile("\\$\\{\\w+\\}").matcher(this.plan.getTargetPath().getFullPath());
    while (m.find()) {
      String param = m.group();
      String value = nodes[Integer.parseInt(param.substring(2, param.length() - 1).trim())];
      m.appendReplacement(sb, value == null ? "" : value);
    }
    m.appendTail(sb);
    return sb.toString();
  }
}
