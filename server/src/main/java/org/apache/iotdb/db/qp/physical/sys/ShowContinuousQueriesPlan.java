package org.apache.iotdb.db.qp.physical.sys;

public class ShowContinuousQueriesPlan extends ShowPlan {

  public ShowContinuousQueriesPlan() {
    super(ShowContentType.CONTINUOUS_QUERY);
  }
}
