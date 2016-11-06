package nakadi;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;

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

  public String eid() {
    return eid;
  }

  public EventMetadata eid(String eid) {
    this.eid = eid;
    return this;
  }

  public EventMetadata newEid() {
    this.eid = ResourceSupport.nextEid();
    return this;
  }

  public String eventType() {
    return eventType;
  }

  public EventMetadata eventType(String eventType) {
    this.eventType = eventType;
    return this;
  }

  public OffsetDateTime occurredAt() {
    return occurredAt;
  }

  public EventMetadata occurredAt(OffsetDateTime occurredAt) {
    this.occurredAt = occurredAt;
    return this;
  }

  public EventMetadata newOccurredAt() {
    this.occurredAt = OffsetDateTime.now();
    return this;
  }

  public OffsetDateTime receivedAt() {
    return receivedAt;
  }

  public List<String> parentEids() {
    return parentEids;
  }

  public EventMetadata parentEids(List<String> parentEids) {
    this.parentEids = parentEids;
    return this;
  }

  public String flowId() {
    return flowId;
  }

  public EventMetadata flowId(String flowId) {
    this.flowId = flowId;
    return this;
  }

  public EventMetadata newFlowId() {
    this.flowId = ResourceSupport.nextFlowId();
    return this;
  }

  public String partition() {
    return partition;
  }

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
