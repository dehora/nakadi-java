package nakadi;

import java.util.Map;

public class Metrics {
  /*
  the api definition is an empty object. we shove the json into this field to gives us something
  to latch onto
   */
  private Map<String, Object> items;

  public Map<String, Object> items() {
    return items;
  }

  Metrics items(Map<String, Object> items) {
    this.items = items;
    return this;
  }
}
