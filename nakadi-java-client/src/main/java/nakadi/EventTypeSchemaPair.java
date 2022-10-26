package nakadi;

import java.util.Objects;

public class EventTypeSchemaPair<T> {

    private String eventTypeName;
    private T schema;
    private String version;

    public EventTypeSchemaPair(){

    }

    public String eventTypeName() {
        return eventTypeName;
    }

    public EventTypeSchemaPair<T> eventTypeName(String eventTypeName) {
        this.eventTypeName = eventTypeName;
        return this;
    }

    public T schema() {
        return schema;
    }

    public EventTypeSchemaPair<T> schema(T schema) {
        this.schema = schema;
        return this;
    }

    public String version() {
        return version;
    }

    public EventTypeSchemaPair<T> version(String version) {
        this.version = version;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventTypeSchemaPair<?> that = (EventTypeSchemaPair<?>) o;

        if (!eventTypeName.equals(that.eventTypeName)) return false;
        if (!schema.equals(that.schema)) return false;
        return Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        int result = eventTypeName.hashCode();
        result = 31 * result + schema.hashCode();
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EventTypeSchemaPair{" +
                "eventTypeName='" + eventTypeName + '\'' +
                ", schema=" + schema +
                ", version='" + version + '\'' +
                '}';
    }
}
