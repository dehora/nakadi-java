package nakadi;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import jdk.nashorn.internal.ir.annotations.Immutable;

public class EventResourceReal implements EventResource {

  private static final String PATH_EVENT_TYPES = "event-types";
  private static final String PATH_COLLECTION = "events";
  private static final String APPLICATION_JSON = "application/json";

  private final NakadiClient client;

  public EventResourceReal(NakadiClient client) {
    this.client = client;
  }

  private static Response timed(Supplier<Response> sender, NakadiClient client, int eventCount) {
    long start = System.nanoTime();
    Response response = null;
    try {
      response = sender.get();
      return response;
    } finally {
      if (response != null) {
        emitMetric(client, response, eventCount);
      }
      client.metricCollector().duration(
          MetricCollector.Timer.eventSend, (System.nanoTime() - start), TimeUnit.NANOSECONDS);
    }
  }

  private static void emitMetric(NakadiClient client, Response response, int eventCount) {
    if (response.statusCode() >= 200 && response.statusCode() <= 204) {
      client.metricCollector().mark(MetricCollector.Meter.sent, eventCount);
    }

    if (response.statusCode() == 207) {
      client.metricCollector().mark(MetricCollector.Meter.http207);
    }
  }

  @Override
  public <T> Response send(String eventTypeName, T event) {
    NakadiException.throwNonNull(eventTypeName, "Please provide an event type name");
    NakadiException.throwNonNull(event, "Please provide an event");
    return send(eventTypeName, ImmutableList.of(event));
  }

  @Override
  public final <T> Response send(String eventTypeName, Collection<T> events) {
    NakadiException.throwNonNull(eventTypeName, "Please provide an event type name");
    NakadiException.throwNonNull(events, "Please provide one or more events");

    if (events.size() == 0) {
      throw new NakadiException(Problem.localProblem("event send called with zero events", ""));
    }

    List<EventRecord<T>> collect =
        events.stream().map(e -> new EventRecord<>(eventTypeName, e)).collect(Collectors.toList());

    return send(collect);
  }

  private <T> Response send(List<EventRecord<T>> events) {
    NakadiException.throwNonNull(events, "Please provide one or more event records");

    String topic = events.get(0).eventType();
    List<Object> eventList =
        events.stream().map(this::mapEventRecordToSerdes).collect(Collectors.toList());

    return timed(() -> {
          ResourceOptions options = options().scope(TokenProvider.NAKADI_EVENT_STREAM_WRITE);
          return client.resourceProvider()
              .newResource()
              .requestThrowing(
                  Resource.POST, collectionUri(topic).buildString(), options, eventList);
        },
        client,
        eventList.size());
  }

  @VisibleForTesting
  <T> Object mapEventRecordToSerdes(EventRecord<T> er) {
    return EventMappedSupport.mapEventRecordToSerdes(er);
  }

  private ResourceOptions options() {
    return ResourceSupport.options(APPLICATION_JSON).tokenProvider(client.resourceTokenProvider());
  }

  private UriBuilder collectionUri(String topic) {
    return UriBuilder.builder(client.baseURI())
        .path(PATH_EVENT_TYPES)
        .path(topic)
        .path(PATH_COLLECTION);
  }
}
