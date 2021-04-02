package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.*;

public class CreateContinuousQueryPlan extends PhysicalPlan implements java.io.Serializable {

  private String querySql;
  private String continuousQueryName;
  private PartialPath targetPath;
  private long everyInterval;
  private long forInterval;
  private QueryOperator queryOperator;

  public CreateContinuousQueryPlan() {
    super(false, Operator.OperatorType.CREATE_CONTINUOUS_QUERY);
  }

  public CreateContinuousQueryPlan(
      String querySql,
      String continuousQueryName,
      PartialPath targetPath,
      long everyInterval,
      long forInterval,
      QueryOperator queryOperator) {
    super(false, Operator.OperatorType.CREATE_CONTINUOUS_QUERY);
    this.querySql = querySql;
    this.continuousQueryName = continuousQueryName;
    this.targetPath = targetPath;
    this.everyInterval = everyInterval;
    this.forInterval = forInterval;
    this.queryOperator = queryOperator;
  }

  public void setQuerySql(String querySql) {
    this.querySql = querySql;
  }

  public String getQuerySql() {
    return querySql;
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

  public void setQueryOperator(QueryOperator queryOperator) {
    this.queryOperator = queryOperator;
  }

  public QueryOperator getQueryOperator() {
    return queryOperator;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_CONTINUOUS_QUERY.ordinal());
    ReadWriteIOUtils.write(continuousQueryName, buffer);
    ReadWriteIOUtils.write(querySql, buffer);
    ReadWriteIOUtils.write(targetPath.getFullPath(), buffer);
    buffer.putLong(everyInterval);
    buffer.putLong(forInterval);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    continuousQueryName = ReadWriteIOUtils.readString(buffer);
    querySql = ReadWriteIOUtils.readString(buffer);
    targetPath = new PartialPath(ReadWriteIOUtils.readString(buffer));
    everyInterval = ReadWriteIOUtils.readLong(buffer);
    forInterval = ReadWriteIOUtils.readLong(buffer);
  }
}
