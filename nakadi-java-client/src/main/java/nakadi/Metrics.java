package nakadi;

import java.util.Map;

/**
 * Represents API server metrics.
 *
 * <p>
 * The api definition is an empty object, the JSON data is placed into the {@link #items} field
 * </p>
 */
public class Metrics {
  private Map<String, Object> items;

  /**
   * @return the metrics
   */
  public Map<String, Object> items() {
    return items;
  }

  Metrics items(Map<String, Object> items) {
    this.items = items;
    return this;
  }
}
