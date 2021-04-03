package org.apache.iotdb.db.exception.metadata;

import org.apache.iotdb.rpc.TSStatusCode;

public class ContinuousQueryAlreadyExistException extends MetadataException {

  private static final long serialVersionUID = -6713847897890531438L;

  public ContinuousQueryAlreadyExistException(String continuousQueryName) {
    super(
        String.format("Continuous Query [%s] already exist", continuousQueryName),
        TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode());
    this.isUserException = true;
  }
}
