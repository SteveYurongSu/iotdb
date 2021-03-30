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
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContinuousQuery implements Runnable {

  private PlanExecutor planExecutor;
  private CreateContinuousQueryPlan plan;
  private List<PartialPath> targetPaths;

  public ContinuousQuery(CreateContinuousQueryPlan plan)
      throws IllegalPathException, QueryProcessException {
    this.plan = plan;
    this.planExecutor = new PlanExecutor();
    setTargetPaths(plan.getDeduplicatedPaths());
  }

  @Override
  public void run() {

    System.out.println("===============schedule===============");

    // construct context

    long queryId =
        QueryResourceManager.getInstance()
            .assignQueryId(true, 1024, plan.getDeduplicatedPaths().size());

    // QueryTimeManager queryTimeManager = QueryTimeManager.getInstance();

    // queryTimeManager.registerQuery(queryId, currentTime, statement, timeout);

    long timestamp = Instant.now().toEpochMilli();

    plan.setStartTime(timestamp - plan.getForInterval());
    plan.setEndTime(timestamp);

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

    // queryTimeManager.unRegisterQuery(queryId);
//
//    int columnSize = result.getPaths().size();
//
//    System.out.println("=============" + columnSize + "===============");
//
//    InsertTabletPlan[] insertTabletPlans = new InsertTabletPlan[columnSize];
//
//    for (int i = 0; i < columnSize; i++) {
//      try {
//        List<String> measurements = Arrays.asList(targetPaths.get(i).getMeasurement());
//        System.out.println(
//            "===========measurement: " + targetPaths.get(i).getMeasurement() + "===============");
//        System.out.println(
//            "===========device: " + targetPaths.get(i).getDevice() + "===============");
//        insertTabletPlans[i] =
//            new InsertTabletPlan(
//                new PartialPath(targetPaths.get(i).getDevice()),
//                measurements,
//                result.getDataTypes());
//      } catch (IllegalPathException e) {
//        e.printStackTrace();
//      }
//    }
//
//    while (true) {
//
//      ColumnsBatch columnsBatch = result.nextColumnsBatch(1024);
//
//      System.out.println("============ row num: " + columnsBatch.getRowNum() + "============");
//
//      if (columnsBatch == null) {
//        break;
//      }
//
//      for (int i = 0; i < columnSize; i++) {
//        insertTabletPlans[i].setTimes(columnsBatch.getTimestampList());
//        insertTabletPlans[i].setColumns(columnsBatch.getColumn(i));
//        insertTabletPlans[i].setRowCount(columnsBatch.getRowNum());
//        try {
//          planExecutor.insertTablet(insertTabletPlans[i]);
//        } catch (QueryProcessException e) {
//          e.printStackTrace();
//        }
//      }
//    }

    int columnSize = result.getDataTypes().size();

    InsertTabletPlan[] insertTabletPlans = new InsertTabletPlan[columnSize];

    for (int i = 0; i < columnSize; i++) {
      List<String> measurements = Arrays.asList(targetPaths.get(i).getMeasurement());
      try {
        insertTabletPlans[i] =
            new InsertTabletPlan(
                new PartialPath(targetPaths.get(i).getDevice()),
                measurements,
                result.getDataTypes());
      } catch (IllegalPathException e) {
        e.printStackTrace();
      }
    }

    int fetchSize = 1024;

    ArrayList<ArrayList<Object>> columnsList = new ArrayList<>(columnSize);
    for (int i = 0; i < columnSize; i++) {
      columnsList.add(new ArrayList<>(fetchSize));
    }
    ArrayList<Long> timestampList = new ArrayList<>(fetchSize);

    try {

      while (true) {
        int insertedRowNum = 0;
        for (ArrayList<Object> objectArrayList : columnsList) {
          objectArrayList.clear();
        }
        timestampList.clear();

        boolean hasNext = true;

        while (true) {
          if (++insertedRowNum > fetchSize) {
            break;
          }
          if (!result.hasNextWithoutConstraint()) {
            hasNext = false;
            break;
          }

          RowRecord r = result.nextWithoutConstraint();

          List<Field> fields = r.getFields();

          Long ts = r.getTimestamp();
          timestampList.add(ts);

          for (ArrayList<Object> objects : columnsList) {
            for (Field f : fields) {
              objects.add(f == null ? null : f.getObjectValue(f.getDataType()));
            }
          }
        }

        if (!timestampList.isEmpty()) {

          for (int i = 0; i < columnSize; i++) {
            insertTabletPlans[i].setTimes(timestampList.stream().mapToLong(t -> t).toArray());
            insertTabletPlans[i].setColumns(columnsList.get(i).toArray());
            insertTabletPlans[i].setRowCount(insertedRowNum);
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

  private void setTargetPaths(List<PartialPath> rawPaths) throws IllegalPathException {
    this.targetPaths = new ArrayList<>(rawPaths.size());
    for (int i = 0; i < rawPaths.size(); i++) {
      this.targetPaths.add(new PartialPath(fillTemplate(rawPaths.get(i))));
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
