package org.apache.iotdb.db.query.dataset;

public class ShowContinuousQueriesResult extends ShowResult {

  private String configuration;

  public ShowContinuousQueriesResult(String name, String configuration) {
    super(name, "");
    this.configuration = configuration;
  }

  public String getConfiguration() {
    return configuration;
  }
}
