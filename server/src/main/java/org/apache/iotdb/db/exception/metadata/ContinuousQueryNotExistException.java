package org.apache.iotdb.db.exception.metadata;

import org.apache.iotdb.rpc.TSStatusCode;

public class ContinuousQueryNotExistException extends MetadataException {

  private static final long serialVersionUID = -6713847897890531438L;

  public ContinuousQueryNotExistException(String continuousQueryName) {
    super(
        String.format("Continuous Query [%s] does not exist", continuousQueryName),
        TSStatusCode.CONTINUOUS_QUERY_NOT_EXIST.getStatusCode());
    this.isUserException = true;
  }
}
