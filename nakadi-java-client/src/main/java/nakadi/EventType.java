package nakadi;

import com.google.common.collect.Lists;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents an event type.
 */
public class EventType {

  public static final String ENRICHMENT_METADATA = "metadata_enrichment";
  public static final String PARTITION_RANDOM = "random";
  public static final String PARTITION_HASH = "hash";
  private static final List<String> SENTINEL_EMPTY_SCOPES =
      Collections.unmodifiableList(Lists.newArrayList());
  private final List<String> enrichmentStrategies = new ArrayList<>();
  private String name;
  private String owningApplication;
  private Category category;
  private String partitionStrategy = PARTITION_RANDOM;
  private EventTypeSchema schema;
  private List<String> partitionKeyFields = new ArrayList<>();
  private EventTypeStatistics defaultStatistic;
  private EventTypeOptions options;
  private String compatibilityMode;
  private EventTypeAuthorization authorization;
  private OffsetDateTime createdAt;
  private OffsetDateTime updatedAt;

  /**
   * @return the event type name
   */
  public String name() {
    return name;
  }

  /**
   * Set  the event type name.
   *
   * @param name the event type name
   * @return this
   */
  public EventType name(String name) {
    NakadiException.throwNonNull(name, "Please provide a non-null name");
    this.name = name;
    return this;
  }

  /**
   * @return the owner of the event type
   */
  public String owningApplication() {
    return owningApplication;
  }

  /**
   * Set the owner of the event type.
   *
   * @param owningApplication the owner of the event type
   * @return this
   */
  public EventType owningApplication(String owningApplication) {
    NakadiException.throwNonNull(owningApplication, "Please provide a non-null owning application");
    this.owningApplication = owningApplication;
    return this;
  }

  /**
   * @return The category of the event type.
   */
  public Category category() {
    return category;
  }

  /**
   * Set the category of the event type.
   *
   * @param category The category of the event type.
   * @return this
   */
  public EventType category(Category category) {
    NakadiException.throwNonNull(category, "Please provide a non-null category");
    this.category = category;
    return this;
  }

  /**
   * @return The enrichments for the event type
   */
  public List<String> enrichmentStrategies() {
    return enrichmentStrategies;
  }

  /**
   * @return the partition strategy for the event type
   */
  public String partitionStrategy() {
    return partitionStrategy;
  }

  /**
   * Set  the partition strategy for the event type.
   *
   * @param partitionStrategy the partition strategy for the event type
   * @return this
   */
  public EventType partitionStrategy(String partitionStrategy) {
    NakadiException.throwNonNull(partitionStrategy, "Please provide a non-null partition strategy");
    this.partitionStrategy = partitionStrategy;
    return this;
  }

  /**
   * @return The schema for the event type
   */
  public EventTypeSchema schema() {
    return schema;
  }

  /**
   * Set the schema for the event type.
   *
   * @param schema The schema for the event type
   * @return this
   */
  public EventType schema(EventTypeSchema schema) {
    NakadiException.throwNonNull(schema, "Please provide non-null schema");
    this.schema = schema;
    return this;
  }

  /**
   * @return the partition key fields
   */
  public List<String> partitionKeyFields() {
    return partitionKeyFields;
  }

  /**
   * Set the partition key fields. Note this will <b>replace was was previously set</b>.
   *
   * @param partitionKeyFields the partition key fields
   * @return this
   */
  public EventType partitionKeyFields(String... partitionKeyFields) {
    NakadiException.throwNonNull(partitionKeyFields,
        "Please provide non-null partition key fields");
    this.partitionKeyFields = Arrays.asList(partitionKeyFields);
    return this;
  }

  /**
   * @return the EventTypeStatistics
   */
  public EventTypeStatistics eventTypeStatistics() {
    return defaultStatistic;
  }

  /**
   * Set the statistic. Note this only is useful when initially creating an event type.
   *
   * @param eventTypeStatistic the statistic to set
   * @return this
   */
  public EventType eventTypeStatistics(EventTypeStatistics eventTypeStatistic) {
    this.defaultStatistic = eventTypeStatistic;
    return this;
  }

  /**
   * Sets the options supported by the API. Note this will <b>replace was was previously set</b>.
   *
   * @param options the options
   * @return this
   */
  public EventType options(EventTypeOptions options) {
    NakadiException.throwNonNull(options, "Please provide non-null event type options");
    this.options = options;
    return this;
  }

  /**
   * @return the options
   */
  public EventTypeOptions options() {
    return options;
  }

  /**
   * Add to the enrichment strategies for the event type.
   *
   * @param enrichmentStrategies the enrichment strategies for the event type
   * @return this.
   */
  public EventType enrichmentStrategies(String... enrichmentStrategies) {
    NakadiException.throwNonNull(enrichmentStrategies,
        "Please provide non-null enrichment strategies");
    this.enrichmentStrategies.addAll(Arrays.asList(enrichmentStrategies));
    return this;
  }

  /**
   * Add an enrichment strategy for the event type.
   *
   * @param enrichmentStrategy an enrichment strategy for the event type
   * @return this.
   */
  public EventType enrichmentStrategy(String enrichmentStrategy) {
    NakadiException.throwNonNull(enrichmentStrategy,
        "Please provide a non-null enrichment strategy");
    this.enrichmentStrategies.add(enrichmentStrategy);
    return this;
  }

  /**
   * Deprecated since 0.9.7 and will be removed in 0.10.0.
   *
   * @return an empty list
   */
  @Deprecated
  public List<String> readScopes() {
    return SENTINEL_EMPTY_SCOPES;
  }

  /**
   * Deprecated since 0.9.7 and will be removed in 0.10.0.
   * <p>
   * Scopes have been removed in recent Nakadi versions. Scopes set here are ignored.
   *</p>
   * @param readScopes the read scopes
   * @return this
   */
  @Deprecated
  public EventType readScopes(String... readScopes) {
    return this;
  }

  /**
   * Deprecated since 0.9.7 and will be removed in 0.10.0.
   *
   * @return an empty list
   */
  @Deprecated
  public List<String> writeScopes() {
    return SENTINEL_EMPTY_SCOPES;
  }

  /**
   * Deprecated since 0.9.7 and will be removed in 0.10.0.
   * <p>
   * Scopes have been removed in recent Nakadi versions. Scopes set here are ignored.
   *</p>
   * @param writeScopes the write scopes
   * @return this
   */
  @Deprecated
  public EventType writeScopes(String... writeScopes) {
    return this;
  }

  /**
   * The compatibility mode of the schema.
   *
   * @return the compatibility mode.
   */
  @Experimental
  public String compatibilityMode() {
    return compatibilityMode;
  }

  /**
   * Set the compatibility mode of the schema.
   *
   * @return this.
   */
  @Experimental
  public EventType compatibilityMode(String compatibilityMode) {
    this.compatibilityMode = compatibilityMode;
    return this;
  }

  /**
   * @return the time the event type was created.
   */
  @Experimental
  public OffsetDateTime createdAt() {
    return createdAt;
  }

  /**
   * @return the time the event type was updated.
   */
  @Experimental
  public OffsetDateTime updatedAt() {
    return updatedAt;
  }

  /**
   *
   * @return the authorization for this event type.
   */
  public EventTypeAuthorization authorization() {
    return authorization;
  }

  /**
   * Set the authorization for this event type.
   *
   * @param authorization the authorization for this event type.
   * @return this
   */
  public EventType authorization(EventTypeAuthorization authorization) {
    this.authorization = authorization;
    return this;
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
        Objects.equals(defaultStatistic, eventType.defaultStatistic) &&
        Objects.equals(options, eventType.options) &&
        Objects.equals(compatibilityMode, eventType.compatibilityMode) &&
        Objects.equals(authorization, eventType.authorization) &&
        Objects.equals(createdAt, eventType.createdAt) &&
        Objects.equals(updatedAt, eventType.updatedAt);
  }

  @Override public String toString() {
    return "EventType{" + "enrichmentStrategies=" + enrichmentStrategies +
        ", name='" + name + '\'' +
        ", owningApplication='" + owningApplication + '\'' +
        ", category=" + category +
        ", partitionStrategy='" + partitionStrategy + '\'' +
        ", schema=" + schema +
        ", partitionKeyFields=" + partitionKeyFields +
        ", defaultStatistic=" + defaultStatistic +
        ", options=" + options +
        ", compatibilityMode='" + compatibilityMode + '\'' +
        ", authorization=" + authorization +
        ", createdAt=" + createdAt +
        ", updatedAt=" + updatedAt +
        '}';
  }

  public enum Category {
    undefined, business, data
  }
}
