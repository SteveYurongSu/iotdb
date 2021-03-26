package org.apache.iotdb.db.sink.alertmanager;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.iotdb.db.sink.api.Configuration;

public class AlertManagerConfiguration implements Configuration {

  private final String host;

  public AlertManagerConfiguration(String host) {

    this.host = host;
  }

  public String getHost() {
    return host;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof org.apache.iotdb.db.sink.alertmanager.AlertManagerConfiguration)) {
      return false;
    }

    org.apache.iotdb.db.sink.alertmanager.AlertManagerConfiguration that =
        (org.apache.iotdb.db.sink.alertmanager.AlertManagerConfiguration) o;

    return new EqualsBuilder().appendSuper(super.equals(o)).append(host, that.host).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).appendSuper(super.hashCode()).append(host).toHashCode();
  }
}
