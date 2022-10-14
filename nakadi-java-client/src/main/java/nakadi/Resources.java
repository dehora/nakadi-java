package nakadi;

import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static nakadi.EventTypeSchema.Type.avro_schema;
/**
 * Allows access to the API via resource classes.
 */
public class Resources {

  private final NakadiClient client;

  Resources(NakadiClient client) {
    this.client = client;
  }

  /**
   * A builder to set up a new event stream. Use to create a consumer for an event stream
   * or a subscription, based on the configuration supplied to the builder.
   *
   * @return a builder for a stream
   */
  public StreamProcessor.Builder streamBuilder() {
    return StreamProcessor.newBuilder(client);
  }

  /**
   * A builder to set up a new event stream based on a {@link StreamConfiguration}. Use to create
   * a consumer for an event stream or a subscription, based on the supplied configuration.
   *
   * @return a builder for a stream
   */
  public StreamProcessor.Builder streamBuilder(StreamConfiguration configuration) {
    return streamBuilder().streamConfiguration(configuration);
  }

  /**
   * The resource for event types
   *
   * @return a resource for working with event types
   */
  public EventTypeResource eventTypes() {
    return new EventTypeResourceReal(client);
  }

  /**
   * The resource for events
   *
   * @return a resource for working with events
   */
  public EventResource events() {
    return new EventResourceReal(client);
  }

  /**
   * The resource for binary events
   *
   * @return a resource for working with events
   */
  public EventResource eventsBinary(EventTypeSchemaPair<Schema>... etSchemaPairs) {

    EventTypeResource etResource = eventTypes();
    Map<String, EventTypeSchemaPair<Schema>> etSchemaMap = new HashMap<>();
    Function<EventTypeSchemaPair<Schema>, Optional<EventTypeSchema>> toSchema =
            (etS) -> etResource.fetchSchema(etS.eventTypeName(), new EventTypeSchema().schema(etS.schema().toString()).type(avro_schema));


    Arrays.stream(etSchemaPairs).
            forEach(etS -> {
              Optional<EventTypeSchema> out = toSchema.apply(etS);
              etSchemaMap.put(etS.eventTypeName(), out.isPresent()? etS.version(out.get().version()): null);
            });
    List<String> etsWithNoMatchingSchema = etSchemaMap.keySet().stream().
            filter(etName -> Objects.isNull(etSchemaMap.get(etName))).collect(Collectors.toList());

    if(!etsWithNoMatchingSchema.isEmpty())
      throw new InvalidSchemaException("No matching schemas found for event types "+ etsWithNoMatchingSchema);

    return new EventResourceReal(client,
            client.jsonSupport(),
            client.compressionSupport(),
            new AvroPayloadSerializer(etSchemaMap));
  }

  /**
   * The resource for subscriptions
   *
   * @return a resource for working with subscriptions
   */
  public SubscriptionResource subscriptions() {
    return new SubscriptionResourceReal(client);
  }

  /**
   * The resource for the schema registry
   *
   * @return a resource for working with the schema registry
   */
  public RegistryResource registry() {
    return new RegistryResource(client);
  }

  /**
   * The resource for health
   *
   * @return a resource for working with health
   */
  public HealthCheckResource health() {
    return new HealthCheckResource(client);
  }

  /**
   * The resource for metrics
   *
   * @return a resource for working with metrics
   */
  public MetricsResource metrics() {
    return new MetricsResource(client);
  }
}
