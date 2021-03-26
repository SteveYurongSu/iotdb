package org.apache.iotdb.db.sink.alertmanager;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.iotdb.db.sink.api.Handler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.iotdb.db.sink.exception.SinkException;

import java.lang.reflect.Type;
import java.util.Map;

public class AlertManagerHandler
    implements Handler<
        org.apache.iotdb.db.sink.alertmanager.AlertManagerConfiguration,
        org.apache.iotdb.db.sink.alertmanager.AlertManagerEvent> {

  private CloseableHttpClient client;
  private HttpPost request;

  @Override
  public void open(org.apache.iotdb.db.sink.alertmanager.AlertManagerConfiguration configuration)
      throws Exception {

    this.client = HttpClients.createDefault();
    this.request = new HttpPost(configuration.getHost());
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
  }

  @Override
  public void onEvent(AlertManagerEvent event) throws Exception {

    String json = eventToJson(event);

    request.setEntity(new StringEntity(json));

    CloseableHttpResponse response = client.execute(request);

    if (response.getStatusLine().getStatusCode() != 200) {
      throw new SinkException(response.getStatusLine().toString());
    }
  }

  private static String eventToJson(AlertManagerEvent event) throws SinkException {
    Gson gson = new Gson();
    Type gsonType = new TypeToken<Map>() {}.getType();

    StringBuilder sb = new StringBuilder();
    sb.append("[{\"labels\":");

    if (event.getLabels() == null) {
      throw new SinkException("labels empty error");
    }

    String labelsString = gson.toJson(event.getLabels(), gsonType);
    sb.append(labelsString);

    if (event.getAnnotations() != null) {
      String annotationsString = gson.toJson(event.getAnnotations(), gsonType);
      sb.append(",");
      sb.append("\"annotations\":");
      sb.append(annotationsString);
    }
    sb.append("}]");
    return sb.toString();
  }
}
