package org.apache.iotdb.db.cq;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContinuousQuery implements Runnable {

  private PlanExecutor planExecutor;
  private CreateContinuousQueryPlan plan;
  private List<PartialPath> targetPaths;

  public ContinuousQuery(CreateContinuousQueryPlan plan) throws QueryProcessException {
    this.plan = plan;
    this.planExecutor = new PlanExecutor();
  }

  @Override
  public void run() {

    System.out.println("===============schedule===============");

    // construct context

    long queryId =
        QueryResourceManager.getInstance()
            .assignQueryId(true, 1024, plan.getDeduplicatedPaths().size());

    long timestamp = Instant.now().toEpochMilli();

    plan.setStartTime(timestamp - plan.getForInterval());
    plan.setEndTime(timestamp);
    try {
      plan.getGroupByTimePlan().setExpression(null);
    } catch (QueryProcessException e) {
      e.printStackTrace();
    }

    QueryDataSet result = null;
    try {
      result = planExecutor.processQuery(plan.getGroupByTimePlan(), new QueryContext(queryId));
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      QueryResourceManager.getInstance().endQuery(queryId);
    } catch (StorageEngineException e) {
      e.printStackTrace();
    }

    try {
      setTargetPaths(result.getPaths());
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }

    int columnSize = result.getDataTypes().size();

    InsertTabletPlan[] insertTabletPlans = new InsertTabletPlan[columnSize];

    String[] measurements = new String[] {targetPaths.get(0).getMeasurement()};
    List<Integer> dataTypes = Collections.singletonList(result.getDataTypes().get(0).ordinal());

    for (int i = 0; i < columnSize; i++) {
      try {
        insertTabletPlans[i] =
            new InsertTabletPlan(
                new PartialPath(targetPaths.get(i).getDevice()), measurements, dataTypes);
      } catch (IllegalPathException e) {
        e.printStackTrace();
      }
    }

    int fetchSize = 100;

    double[][][] columns = new double[columnSize][1][fetchSize];
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
              columns[i][0][rowNums[i]] = f.getDoubleV();
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



  private void setTargetPaths(List<Path> rawPaths) throws IllegalPathException {
    this.targetPaths = new ArrayList<>(rawPaths.size());
    for (int i = 0; i < rawPaths.size(); i++) {
      this.targetPaths.add(new PartialPath(fillTemplate((PartialPath) rawPaths.get(i))));
    }
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
