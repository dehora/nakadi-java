package nakadi;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * API event metadata as used by some categories.
 */
public class EventMetadata {

  private String eid;
  private String eventType;
  private OffsetDateTime occurredAt;
  private OffsetDateTime receivedAt;
  private List<String> parentEids;
  private String flowId;
  private String partition;

  public EventMetadata() {
    this.flowId = ResourceSupport.nextFlowId();
    this.eid = ResourceSupport.nextEid();
    this.occurredAt = OffsetDateTime.now();
  }

  /**
   * @return the event identifier
   */
  public String eid() {
    return eid;
  }

  /**
   * Set the event identifier.
   *
   * @param eid the event identifier
   * @return this
   */
  public EventMetadata eid(String eid) {
    this.eid = eid;
    return this;
  }

  EventMetadata newEid() {
    this.eid = ResourceSupport.nextEid();
    return this;
  }

  /**
   * @return the event type name
   */
  public String eventType() {
    return eventType;
  }

  /**
   * Set  the event type name.
   *
   * @param eventType the event type name
   * @return this
   */
  public EventMetadata eventType(String eventType) {
    this.eventType = eventType;
    return this;
  }

  /**
   *
   * @return the time of the event according to the producer
   */
  public OffsetDateTime occurredAt() {
    return occurredAt;
  }

  /**
   * Set the time of the event according to the producer.
   *
   * @param occurredAt the time of the event according to the producer
   * @return this
   */
  public EventMetadata occurredAt(OffsetDateTime occurredAt) {
    this.occurredAt = occurredAt;
    return this;
  }

  EventMetadata newOccurredAt() {
    this.occurredAt = OffsetDateTime.now();
    return this;
  }

  /**
   * The time the broker received the event.
   *
   * @return this
   */
  public OffsetDateTime receivedAt() {
    return receivedAt;
  }

  /**
   * The parent event identifiers for this event.
   *
   * @return parent event identifiers for this event
   */
  public List<String> parentEids() {
    return parentEids;
  }

  /**
   * Set the parent event identifiers for this event.
   *
   * @param parentEids parent event identifiers for this event.
   * @return this
   */
  public EventMetadata parentEids(String... parentEids) {
    this.parentEids = Arrays.asList(parentEids);
    return this;
  }

  /**
   * The flow id associate with this event.
   *
   * @return flow id associate with this event
   */
  public String flowId() {
    return flowId;
  }

  /**
   * Set the flow id associate with this event.
   *
   * @param flowId flow id associate with this event
   * @return this
   */
  public EventMetadata flowId(String flowId) {
    this.flowId = flowId;
    return this;
  }

  EventMetadata newFlowId() {
    this.flowId = ResourceSupport.nextFlowId();
    return this;
  }

  /**
   * @return the partition for the event
   */
  public String partition() {
    return partition;
  }

  /**
   * Set the partition for the event.
   *
   * @param partition the partition for the event
   * @return this
   */
  public EventMetadata partition(String partition) {
    this.partition = partition;
    return this;
  }

  @Override public int hashCode() {
    return Objects.hash(eid, eventType, occurredAt, receivedAt, parentEids, flowId, partition);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventMetadata that = (EventMetadata) o;
    return Objects.equals(eid, that.eid) &&
        Objects.equals(eventType, that.eventType) &&
        Objects.equals(occurredAt, that.occurredAt) &&
        Objects.equals(receivedAt, that.receivedAt) &&
        Objects.equals(parentEids, that.parentEids) &&
        Objects.equals(flowId, that.flowId) &&
        Objects.equals(partition, that.partition);
  }

  @Override public String toString() {
    return "EventMetadata{" + "eid='" + eid + '\'' +
        ", eventType='" + eventType + '\'' +
        ", occurredAt=" + occurredAt +
        ", receivedAt=" + receivedAt +
        ", parentEids=" + parentEids +
        ", flowId='" + flowId + '\'' +
        ", partition='" + partition + '\'' +
        '}';
  }
}
