package nakadi;

import java.util.Objects;

import static nakadi.EventTypeSchema.Type.json_schema;

public class EventTypeSchema {

  private Type type = json_schema;
  private String schema;

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

  @Override public int hashCode() {
    return Objects.hash(type, schema);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventTypeSchema that = (EventTypeSchema) o;
    return type == that.type &&
        Objects.equals(schema, that.schema);
  }

  @Override public String toString() {
    return "EventTypeSchema{" + "type=" + type +
        ", schema='" + schema + '\'' +
        '}';
  }

  public enum Type {
    json_schema
  }
}
