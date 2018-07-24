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
  private String version;
  private String partitionCompactionKey;

  /**
   * Create a new EventMetadata prepared with values for eid, occurred at, and flow id.
   * @return an EventMetadata
   */
  public static EventMetadata newPreparedEventMetadata() {
    return new EventMetadata().withEid().withFlowId().withOccurredAt();
  }

  /**
   * Create a new EventMetadata.
   * <p>
   *   The object is <b>not</b> prepared with any values, such eid, occurred at, and flow id. To
   *   create an EventMetadata with prepared values use {@link #newPreparedEventMetadata()}
   * </p>
   * @return an EventMetadata
   */
  public EventMetadata() {
  }

  /**
   * Set an event identifier on this metadata.
   * @return this
   */
  public EventMetadata withEid() {
    return newEid();
  }

  /**
   * Set an occurred at time on this metadata.
   * @return this
   */
  public EventMetadata withOccurredAt() {
    return newOccurredAt();
  }

  /**
   * Set a flow id on this metadata.
   * @return this
   */
  public EventMetadata withFlowId() {
    return newFlowId();
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
   * @return the event partition compaction key
   */
  public String partitionCompactionKey() {
    return this.partitionCompactionKey;
  }

  /**
   * Set the event partition compaction key
   *
   * @param partitionCompactionKey is the string used for compaction of events
   * @return this
   */
  public EventMetadata partitionCompactionKey(String partitionCompactionKey) {
    this.partitionCompactionKey = partitionCompactionKey;
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

  /**
   * The version of the schema used to validate this event.
   *
   * @return the version.
   */
  @Experimental
  public String version() {
    return version;
  }

  @Override public int hashCode() {
    return Objects.hash(eid, eventType, occurredAt, receivedAt, parentEids, flowId, partition,
        version);
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
        Objects.equals(partition, that.partition) &&
        Objects.equals(version, that.version);
  }

  @Override public String toString() {
    return "EventMetadata{" + "eid='" + eid + '\'' +
        ", eventType='" + eventType + '\'' +
        ", occurredAt=" + occurredAt +
        ", receivedAt=" + receivedAt +
        ", parentEids=" + parentEids +
        ", flowId='" + flowId + '\'' +
        ", partition='" + partition + '\'' +
        ", version='" + version + '\'' +
        '}';
  }
}
