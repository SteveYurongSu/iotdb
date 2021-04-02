package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class CreateContinuousQueryPlan extends PhysicalPlan implements java.io.Serializable {

  private String sql;
  private GroupByTimePlan groupByTimePlan;
  private String continuousQueryName;
  private PartialPath targetPath;
  private long everyInterval;
  private long forInterval;

  public CreateContinuousQueryPlan() {
    super(false, Operator.OperatorType.CREATE_CONTINUOUS_QUERY);
  }

  public CreateContinuousQueryPlan(
      String sql,
      String continuousQueryName,
      PartialPath targetPath,
      long everyInterval,
      long forInterval,
      GroupByTimePlan groupByTimePlan) {
    super(false, Operator.OperatorType.CREATE_CONTINUOUS_QUERY);
    this.sql = sql;
    this.continuousQueryName = continuousQueryName;
    this.targetPath = targetPath;
    this.everyInterval = everyInterval;
    this.forInterval = forInterval;
    this.groupByTimePlan = groupByTimePlan;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public String getSql() {
    return sql;
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

//  @Override
//  public void serialize(DataOutputStream stream) throws IOException {
//    stream.writeByte((byte) PhysicalPlanType.CREATE_CONTINUOUS_QUERY.ordinal());
//    ReadWriteIOUtils.write(sql, stream);
//    ReadWriteIOUtils.write(continuousQueryName, stream);
//    ReadWriteIOUtils.write(targetPath.getFullPath(), stream);
//    stream.writeLong(everyInterval);
//    stream.writeLong(forInterval);
//
//    // group by time plan
//    stream.writeLong(groupByTimePlan.getInterval());
//    stream.writeLong(groupByTimePlan.getSlidingStep());
//    stream.writeBoolean(groupByTimePlan.isIntervalByMonth());
//    stream.writeBoolean(groupByTimePlan.isSlidingStepByMonth());
//    stream.writeBoolean(groupByTimePlan.isLeftCRightO());
//
//    List<String> aggregations = groupByTimePlan.getAggregations();
//    stream.writeInt(aggregations.size());
//    for(int i = 0; i < aggregations.size(); i++) {
//      ReadWriteIOUtils.write(aggregations.get(i), stream);
//    }
//
//    List<String> deduplicatedAggrs = groupByTimePlan.getDeduplicatedAggregations();
//    stream.writeInt(deduplicatedAggrs.size());
//    for(int i = 0; i < deduplicatedAggrs.size(); i++) {
//      ReadWriteIOUtils.write(deduplicatedAggrs.get(i), stream);
//    }
//
//    List<PartialPath> deduplicatedPaths = groupByTimePlan.getDeduplicatedPaths();
//    stream.writeInt(deduplicatedPaths.size());
//    for(int i = 0; i < deduplicatedPaths.size(); i++) {
//      ReadWriteIOUtils.write(deduplicatedPaths.get(i).getFullPath(), stream);
//    }
//
//    List<TSDataType> deduplicatedDataTypes = groupByTimePlan.getDeduplicatedDataTypes();
//    stream.writeInt(deduplicatedDataTypes.size());
//    for(int i = 0; i < deduplicatedDataTypes.size(); i++) {
//      ReadWriteIOUtils.write(deduplicatedDataTypes.get(i).ordinal(), stream);
//    }
//
//    Map<String, Integer> pathToIndex = groupByTimePlan.getPathToIndex();
//    stream.writeInt(pathToIndex.size());
//    for (Map.Entry<String, Integer> entry : pathToIndex.entrySet()) {
//      ReadWriteIOUtils.write(entry.getKey(), stream);
//      stream.writeInt(entry.getValue());
//    }
//
//    stream.writeLong(index);
//  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_CONTINUOUS_QUERY.ordinal());
    ReadWriteIOUtils.write(sql, buffer);
    ReadWriteIOUtils.write(continuousQueryName, buffer);
    ReadWriteIOUtils.write(targetPath.getFullPath(), buffer);
    buffer.putLong(everyInterval);
    buffer.putLong(forInterval);

    // group by time plan
    buffer.putLong(groupByTimePlan.getInterval());
    buffer.putLong(groupByTimePlan.getSlidingStep());
    ReadWriteIOUtils.write(groupByTimePlan.isIntervalByMonth(), buffer);
    ReadWriteIOUtils.write(groupByTimePlan.isSlidingStepByMonth(), buffer);
    ReadWriteIOUtils.write(groupByTimePlan.isLeftCRightO(), buffer);

    List<String> aggregations = groupByTimePlan.getAggregations();
    buffer.putInt(aggregations.size());
    for(int i = 0; i < aggregations.size(); i++) {
      ReadWriteIOUtils.write(aggregations.get(i), buffer);
    }

    List<String> deduplicatedAggrs = groupByTimePlan.getDeduplicatedAggregations();
    buffer.putInt(deduplicatedAggrs.size());
    for(int i = 0; i < deduplicatedAggrs.size(); i++) {
      ReadWriteIOUtils.write(deduplicatedAggrs.get(i), buffer);
    }

    List<PartialPath> deduplicatedPaths = groupByTimePlan.getDeduplicatedPaths();
    buffer.putInt(deduplicatedPaths.size());
    for(int i = 0; i < deduplicatedPaths.size(); i++) {
      ReadWriteIOUtils.write(deduplicatedPaths.get(i).getFullPath(), buffer);
    }

    List<TSDataType> deduplicatedDataTypes = groupByTimePlan.getDeduplicatedDataTypes();
    buffer.putInt(deduplicatedDataTypes.size());
    for(int i = 0; i < deduplicatedDataTypes.size(); i++) {
      ReadWriteIOUtils.write(deduplicatedDataTypes.get(i), buffer);
    }

    Map<String, Integer> pathToIndex = groupByTimePlan.getPathToIndex();
    buffer.putInt(pathToIndex.size());
    for (Map.Entry<String, Integer> entry : pathToIndex.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), buffer);
      buffer.putInt(entry.getValue());
    }

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    sql = ReadWriteIOUtils.readString(buffer);
    continuousQueryName = ReadWriteIOUtils.readString(buffer);
    targetPath = new PartialPath(ReadWriteIOUtils.readString(buffer));
    everyInterval = ReadWriteIOUtils.readLong(buffer);
    forInterval = ReadWriteIOUtils.readLong(buffer);

    // group by time plan
    groupByTimePlan = new GroupByTimePlan();
    groupByTimePlan.setInterval(ReadWriteIOUtils.readLong(buffer));
    groupByTimePlan.setSlidingStep(ReadWriteIOUtils.readLong(buffer));
    groupByTimePlan.setIntervalByMonth(ReadWriteIOUtils.readBool(buffer));
    groupByTimePlan.setSlidingStepByMonth(ReadWriteIOUtils.readBool(buffer));
    groupByTimePlan.setLeftCRightO(ReadWriteIOUtils.readBool(buffer));

    groupByTimePlan.setAggregations(ReadWriteIOUtils.readStringList(buffer));

    groupByTimePlan.setDeduplicatedAggregations(ReadWriteIOUtils.readStringList(buffer));

    int length = ReadWriteIOUtils.readInt(buffer);
    for(int i = 0; i < length; i++) {
      groupByTimePlan.addDeduplicatedPaths(new PartialPath(ReadWriteIOUtils.readString(buffer)));
    }

    length = ReadWriteIOUtils.readInt(buffer);
    for(int i = 0; i < length; i++) {
      groupByTimePlan.addDeduplicatedDataTypes(ReadWriteIOUtils.readDataType(buffer));
    }

    length = ReadWriteIOUtils.readInt(buffer);
    for(int i = 0; i < length; i++) {
      groupByTimePlan.addPathToIndex(ReadWriteIOUtils.readString(buffer), ReadWriteIOUtils.readInt(buffer));
    }

    index = ReadWriteIOUtils.readLong(buffer);
  }
}
