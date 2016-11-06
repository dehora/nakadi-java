package nakadi;

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
    return new EventTypeResource(client);
  }

  /**
   * The resource for events
   *
   * @return a resource for working with events
   */
  public EventResource events() {
    return new EventResource(client);
  }

  /**
   * The resource for subscriptions
   *
   * @return a resource for working with subscriptions
   */
  public SubscriptionResource subscriptions() {
    return new SubscriptionResource(client);
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
