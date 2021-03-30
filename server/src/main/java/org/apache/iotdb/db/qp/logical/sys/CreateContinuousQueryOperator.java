package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;

import java.time.Duration;

public class CreateContinuousQueryOperator extends RootOperator {

  private QueryOperator queryOperator;
  private String continuousQueryName;
  private PartialPath targetPath;
  private long everyInterval;
  private long forInterval;

  public CreateContinuousQueryOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.CREATE_CONTINUOUS_QUERY;
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
}
