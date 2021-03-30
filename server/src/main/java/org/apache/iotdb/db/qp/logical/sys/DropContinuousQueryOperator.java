package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.qp.logical.RootOperator;

public class DropContinuousQueryOperator extends RootOperator {

  private String continuousQueryName;

  public DropContinuousQueryOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.DROP_CONTINUOUS_QUERY;
  }

  public void setContinuousQueryName(String continuousQueryName) {
    this.continuousQueryName = continuousQueryName;
  }

  public String getContinuousQueryName() {
    return continuousQueryName;
  }
}
