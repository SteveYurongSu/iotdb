package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.List;

public class DropContinuousQueryPlan extends PhysicalPlan {

  private final String continuousQueryName;

  public DropContinuousQueryPlan(String continuousQueryName) {
    super(false);
    this.continuousQueryName = continuousQueryName;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  public String getContinuousQueryName() {
    return continuousQueryName;
  }
}
