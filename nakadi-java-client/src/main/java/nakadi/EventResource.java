package nakadi;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class EventResource {

  private static final String PATH_EVENT_TYPES = "event-types";
  private static final String PATH_COLLECTION = "events";
  private static final String APPLICATION_JSON = "application/json";

  private final NakadiClient client;

  public EventResource(NakadiClient client) {
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

  @SafeVarargs public final <T extends Event> Response send(String eventTypeName, T... evts) {
    return send(eventTypeName, Arrays.asList(evts));
  }

  public final Response send(String eventTypeName, List<? extends Event> events) {
    if (events.size() == 0) {
      throw new NakadiException(Problem.localProblem("event send called with zero events", ""));
    }

    List<EventRecord<? extends Event>> collect =
        events.stream().map(e -> new EventRecord<>(eventTypeName, e)
        ).collect(Collectors.toList());

    return send(collect);
  }


  public final Response send(List<EventRecord<? extends Event>> records) {

    String topic = records.get(0).topic();
    List<Object> eventList =
        records.stream().map(this::mapEventRecordToSerdes).collect(Collectors.toList());

    return timed(() ->
            client.resourceProvider()
                .newResource()
                .requestThrowing(
                    Resource.POST, collectionUri(topic).buildString(), options(), eventList),
        client,
        eventList.size());
  }

  @SafeVarargs public final Response send(EventRecord<? extends Event>... events) {
    if (events.length == 0) {
      throw new NakadiException(Problem.localProblem("event send called with zero events", ""));
    }

    List<EventRecord<? extends Event>> records = Arrays.asList(events);
    return send(records);
  }

  @VisibleForTesting
  Object mapEventRecordToSerdes(EventRecord<? extends Event> er) {
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
