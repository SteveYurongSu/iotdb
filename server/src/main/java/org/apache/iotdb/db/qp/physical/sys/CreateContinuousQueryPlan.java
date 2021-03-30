package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

public class CreateContinuousQueryPlan extends PhysicalPlan {

  private GroupByTimePlan groupByTimePlan;
  private String continuousQueryName;
  private PartialPath targetPath;
  private long everyInterval;
  private long forInterval;

  public CreateContinuousQueryPlan() {
    super(false, Operator.OperatorType.CREATE_CONTINUOUS_QUERY);
  }

  public CreateContinuousQueryPlan(
      String continuousQueryName,
      PartialPath targetPath,
      long everyInterval,
      long forInterval,
      GroupByTimePlan groupByTimePlan) {
    super(false, Operator.OperatorType.CREATE_CONTINUOUS_QUERY);
    this.continuousQueryName = continuousQueryName;
    this.targetPath = targetPath;
    this.everyInterval = everyInterval;
    this.forInterval = forInterval;
    this.groupByTimePlan = groupByTimePlan;
  }

  public void setContinuousQueryName(String continuousQueryName) {
    this.continuousQueryName = continuousQueryName;
  }

  public String getContinuousQueryName() {
    return continuousQueryName;
  }

  public void setTargetPath(PartialPath targetPath) {
    this.targetPath = targetPath;
  }

  public PartialPath getTargetPath() {
    return targetPath;
  }

  public void setEveryInterval(long everyInterval) {
    this.everyInterval = everyInterval;
  }

  public long getEveryInterval() {
    return everyInterval;
  }

  public void setForInterval(long forInterval) {
    this.forInterval = forInterval;
  }

  public long getForInterval() {
    return forInterval;
  }

  public List<PartialPath> getDeduplicatedPaths() {
    return groupByTimePlan.getDeduplicatedPaths();
  }

  public void setStartTime(long timestamp) {
    groupByTimePlan.setStartTime(timestamp);
  }

  public void setEndTime(long timestamp) {
    groupByTimePlan.setEndTime(timestamp);
  }

  public void setGroupByTimePlan(GroupByTimePlan plan) {
    this.groupByTimePlan = plan;
  }

  public GroupByTimePlan getGroupByTimePlan() {
    return groupByTimePlan;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }
}
