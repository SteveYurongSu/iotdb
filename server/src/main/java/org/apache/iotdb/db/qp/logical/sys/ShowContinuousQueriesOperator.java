package org.apache.iotdb.db.qp.logical.sys;

public class ShowContinuousQueriesOperator extends ShowOperator {

  public ShowContinuousQueriesOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.SHOW_CONTINUOUS_QUERIES;
  }
}
