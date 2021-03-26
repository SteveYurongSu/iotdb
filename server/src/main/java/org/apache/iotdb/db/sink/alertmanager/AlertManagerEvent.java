package org.apache.iotdb.db.sink.alertmanager;

import org.apache.iotdb.db.sink.api.Event;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AlertManagerEvent implements Event {

  private final Map<String, String> labels;

  private final Map<String, String> annotations;

  public AlertManagerEvent(Map<String, String> labels) {

    this.labels = labels;
    this.annotations = null;
  }

  public AlertManagerEvent(Map<String, String> labels, Map<String, String> annotations) {

    this.labels = labels;
    this.annotations = new HashMap<>();

    for (Map.Entry<String, String> entry : annotations.entrySet()) {
      this.annotations.put(entry.getKey(), fillTemplate(this.labels, entry.getValue()));
    }
  }

  public Map<String, String> getAnnotations() {
    return annotations;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  private static String fillTemplate(Map<String, String> map, String template) {
    if (template == null || map == null) return null;
    StringBuffer sb = new StringBuffer();
    Matcher m = Pattern.compile("\\{\\{\\.\\w+}}").matcher(template);
    while (m.find()) {
      String param = m.group();
      String key = param.substring(3, param.length() - 2).trim();
      String value = map.get(key);
      m.appendReplacement(sb, value == null ? "" : value);
    }
    m.appendTail(sb);
    return sb.toString();
  }
}
