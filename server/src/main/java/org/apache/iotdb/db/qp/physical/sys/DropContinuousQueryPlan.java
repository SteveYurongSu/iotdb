package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.List;

public class DropContinuousQueryPlan extends PhysicalPlan {

  private String continuousQueryName;

  public DropContinuousQueryPlan() {
    super(false, Operator.OperatorType.DROP_CONTINUOUS_QUERY);
  }

  public DropContinuousQueryPlan(String continuousQueryName) {
    super(false, Operator.OperatorType.DROP_CONTINUOUS_QUERY);
    this.continuousQueryName = continuousQueryName;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  public String getContinuousQueryName() {
    return continuousQueryName;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.DROP_CONTINUOUS_QUERY.ordinal());
    ReadWriteIOUtils.write(continuousQueryName, buffer);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    continuousQueryName = ReadWriteIOUtils.readString(buffer);
  }
}
