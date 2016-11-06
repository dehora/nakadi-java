package nakadi;

import com.google.common.collect.Lists;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Subscription {

  private String id;
  private String owningApplication;
  private String consumerGroup;
  private String readFrom;
  private List<String> eventTypes = Lists.newArrayList();
  private OffsetDateTime createdAt;

  public String id() {
    return id;
  }

  public String owningApplication() {
    return owningApplication;
  }

  public Subscription owningApplication(String owningApplication) {
    this.owningApplication = owningApplication;
    return this;
  }

  public String consumerGroup() {
    return consumerGroup;
  }

  public Subscription consumerGroup(String consumerGroup) {
    this.consumerGroup = consumerGroup;
    return this;
  }

  public String readFrom() {
    return readFrom;
  }

  public Subscription readFrom(String readFrom) {
    this.readFrom = readFrom;
    return this;
  }

  public List<String> eventTypes() {
    return eventTypes;
  }

  public Subscription eventTypes(String... eventTypeNames) {
    this.eventTypes.addAll(Arrays.asList(eventTypeNames));
    return this;
  }

  public Subscription eventType(String eventTypeName) {
    this.eventTypes.add(eventTypeName);
    return this;
  }

  public OffsetDateTime createdAt() {
    return createdAt;
  }

  @Override public int hashCode() {
    return Objects.hash(id, owningApplication, consumerGroup, readFrom, eventTypes, createdAt);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Subscription that = (Subscription) o;
    return Objects.equals(id, that.id) &&
        Objects.equals(owningApplication, that.owningApplication) &&
        Objects.equals(consumerGroup, that.consumerGroup) &&
        Objects.equals(readFrom, that.readFrom) &&
        Objects.equals(eventTypes, that.eventTypes) &&
        Objects.equals(createdAt, that.createdAt);
  }

  @Override public String toString() {
    return "Subscription{" + "id='" + id + '\'' +
        ", owningApplication='" + owningApplication + '\'' +
        ", consumerGroup='" + consumerGroup + '\'' +
        ", readFrom='" + readFrom + '\'' +
        ", eventTypes=" + eventTypes +
        ", createdAt=" + createdAt +
        '}';
  }
}
