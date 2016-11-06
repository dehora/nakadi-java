package nakadi;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class EventType {

  public static final String ENRICHMENT_METADATA = "metadata_enrichment";
  public static final String PARTITION_RANDOM = "random";
  public static final String PARTITION_HASH = "hash";
  private final List<String> enrichmentStrategies = Lists.newArrayList();
  private String name;
  private String owningApplication;
  private Category category;
  private String partitionStrategy = PARTITION_RANDOM;
  private EventTypeSchema schema;
  private List<String> partitionKeyFields = Lists.newArrayList();
  private EventTypeStatistics eventTypeStatistics;
  private EventTypeOptions options;
  private List<String> readScopes = Lists.newArrayList();
  private List<String> writeScopes = Lists.newArrayList();

  public String name() {
    return name;
  }

  public EventType name(String name) {
    NakadiException.throwNonNull(options, "Please provide a non-null name");
    this.name = name;
    return this;
  }

  public String owningApplication() {
    return owningApplication;
  }

  public EventType owningApplication(String owningApplication) {
    NakadiException.throwNonNull(options, "Please provide a non-null owning application");
    this.owningApplication = owningApplication;
    return this;
  }

  public Category category() {
    return category;
  }

  public EventType category(Category category) {
    NakadiException.throwNonNull(options, "Please provide a non-null category");
    this.category = category;
    return this;
  }

  public List<String> enrichmentStrategies() {
    return enrichmentStrategies;
  }

  public String partitionStrategy() {
    return partitionStrategy;
  }

  public EventType partitionStrategy(String partitionStrategy) {
    NakadiException.throwNonNull(options, "Please provide a non-null partition strategy");
    this.partitionStrategy = partitionStrategy;
    return this;
  }

  public EventTypeSchema schema() {
    return schema;
  }

  public EventType schema(EventTypeSchema schema) {
    NakadiException.throwNonNull(options, "Please provide non-null schema");
    this.schema = schema;
    return this;
  }

  public List<String> partitionKeyFields() {
    return partitionKeyFields;
  }

  public EventType partitionKeyFields(String... partitionKeyFields) {
    NakadiException.throwNonNull(options, "Please provide non-null partition key fields");
    this.partitionKeyFields = Arrays.asList(partitionKeyFields);
    return this;
  }

  public EventTypeStatistics eventTypeStatistics() {
    return eventTypeStatistics;
  }

  public EventType options(EventTypeOptions options) {
    NakadiException.throwNonNull(options, "Please provide non-null event type options");
    this.options = options;
    return this;
  }

  public EventTypeOptions options() {
    return options;
  }

  public EventType enrichmentStrategies(String... enrichmentStrategies) {
    NakadiException.throwNonNull(enrichmentStrategies,
        "Please provide non-null enrichment strategies");
    this.enrichmentStrategies.addAll(Arrays.asList(enrichmentStrategies));
    return this;
  }

  public EventType enrichmentStrategy(String enrichmentStrategy) {
    NakadiException.throwNonNull(options, "Please provide a non-null enrichment strategy");
    this.enrichmentStrategies.add(enrichmentStrategy);
    return this;
  }

  public List<String> readScopes() {
    return readScopes;
  }

  public EventType readScopes(String... readScopes) {
    NakadiException.throwNonNull(options, "Please provide non-null read scopes");
    this.readScopes = Arrays.asList(readScopes);
    return this;
  }

  public List<String> writeScopes() {
    return writeScopes;
  }

  public EventType writeScopes(String... writeScopes) {
    NakadiException.throwNonNull(options, "Please provide non-null write scopes");
    this.writeScopes = Arrays.asList(writeScopes);
    return this;
  }

  @Override public int hashCode() {
    return Objects.hash(enrichmentStrategies, name, owningApplication, category, partitionStrategy,
        schema, partitionKeyFields, eventTypeStatistics, options, readScopes, writeScopes);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventType eventType = (EventType) o;
    return Objects.equals(enrichmentStrategies, eventType.enrichmentStrategies) &&
        Objects.equals(name, eventType.name) &&
        Objects.equals(owningApplication, eventType.owningApplication) &&
        category == eventType.category &&
        Objects.equals(partitionStrategy, eventType.partitionStrategy) &&
        Objects.equals(schema, eventType.schema) &&
        Objects.equals(partitionKeyFields, eventType.partitionKeyFields) &&
        Objects.equals(eventTypeStatistics, eventType.eventTypeStatistics) &&
        Objects.equals(options, eventType.options) &&
        Objects.equals(readScopes, eventType.readScopes) &&
        Objects.equals(writeScopes, eventType.writeScopes);
  }

  @Override public String toString() {
    return "EventType{" + "enrichmentStrategies=" + enrichmentStrategies +
        ", name='" + name + '\'' +
        ", owningApplication='" + owningApplication + '\'' +
        ", category=" + category +
        ", partitionStrategy='" + partitionStrategy + '\'' +
        ", schema=" + schema +
        ", partitionKeyFields=" + partitionKeyFields +
        ", eventTypeStatistics=" + eventTypeStatistics +
        ", options=" + options +
        ", readScopes=" + readScopes +
        ", writeScopes=" + writeScopes +
        '}';
  }

  public enum Category {
    undefined, business, data
  }
}
