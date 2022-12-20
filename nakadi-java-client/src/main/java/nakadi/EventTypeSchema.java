package nakadi;

import java.time.OffsetDateTime;
import java.util.Objects;

import static nakadi.EventTypeSchema.Type.json_schema;

public class EventTypeSchema {

  private Type type = json_schema;
  private String schema;
  private String version;
  private OffsetDateTime createdAt;

  public Type type() {
    return type;
  }

  public EventTypeSchema type(Type type) {
    this.type = type;
    return this;
  }

  public String schema() {
    return schema;
  }

  public EventTypeSchema schema(String schema) {
    this.schema = schema;
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

  public EventTypeSchema version(String version) {
    this.version = version;
    return this;
  }

  /**
   * @return the time the event type was created.
   */
  @Experimental
  public OffsetDateTime createdAt() {
    return createdAt;
  }

  @Override public int hashCode() {
    return Objects.hash(type, schema, version, createdAt);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventTypeSchema that = (EventTypeSchema) o;
    return type == that.type &&
        Objects.equals(schema, that.schema) &&
        Objects.equals(version, that.version) &&
        Objects.equals(createdAt, that.createdAt);
  }

  @Override public String toString() {
    return "EventTypeSchema{" + "type=" + type +
        ", schema='" + schema + '\'' +
        ", version='" + version + '\'' +
        ", createdAt=" + createdAt +
        '}';
  }

  public enum Type {
    json_schema,
    avro_schema
  }
}
