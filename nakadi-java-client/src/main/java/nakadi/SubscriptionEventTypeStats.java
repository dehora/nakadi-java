package nakadi;

import java.util.List;
import java.util.Objects;

/**
 * Represents the event tyoe stats for an API {@link Subscription}.
 *
 * @see nakadi.Subscription
 */
public class SubscriptionEventTypeStats {

  private String eventType;
  private List<Partition> partitions;

  /**
   * @return the event type
   */
  public String eventType() {
    return eventType;
  }

  /**
   * @return the partitions
   */
  public List<Partition> partitions() {
    return partitions;
  }

  @Override public int hashCode() {
    return Objects.hash(eventType, partitions);
  }

  public static class Partition {

    private String partition;
    private String state;
    private String unconsumed_events;
    private String client_id;

    public String partition() {
      return partition;
    }

    public String state() {
      return state;
    }

    public String unconsumedEvents() {
      return unconsumed_events;
    }

    public String clientId() {
      return client_id;
    }

    @Override public String toString() {
      return "Partition{" + "partition='" + partition + '\'' +
          ", state='" + state + '\'' +
          ", unconsumed_events='" + unconsumed_events + '\'' +
          ", client_id='" + client_id + '\'' +
          '}';
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Partition partition1 = (Partition) o;
      return Objects.equals(partition, partition1.partition) &&
          Objects.equals(state, partition1.state) &&
          Objects.equals(unconsumed_events, partition1.unconsumed_events) &&
          Objects.equals(client_id, partition1.client_id);
    }

    @Override public int hashCode() {
      return Objects.hash(partition, state, unconsumed_events, client_id);
    }
  }

  @Override public String toString() {
    return "SubscriptionEventTypeStats{" + "eventType='" + eventType + '\'' +
        ", partitions=" + partitions +
        '}';
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubscriptionEventTypeStats that = (SubscriptionEventTypeStats) o;
    return Objects.equals(eventType, that.eventType) &&
        Objects.equals(partitions, that.partitions);
  }
}
