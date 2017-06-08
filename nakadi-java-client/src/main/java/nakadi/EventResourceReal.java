package nakadi;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventResourceReal implements EventResource {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private static final String PATH_EVENT_TYPES = "event-types";
  private static final String PATH_COLLECTION = "events";
  private static final String APPLICATION_JSON = "application/json";

  private static final List<ResourceLink> LINKS_SENTINEL = Lists.newArrayList();
  private static Type TYPE_BIR = new TypeToken<List<BatchItemResponse>>() {
  }.getType();

  private final NakadiClient client;
  private final JsonSupport jsonSupport;
  private volatile String scope;
  private volatile RetryPolicy retryPolicy;
  private volatile String flowId;

  public EventResourceReal(NakadiClient client) {
    this(client, client.jsonSupport());
  }

  @VisibleForTesting
  EventResourceReal(NakadiClient client, JsonSupport jsonSupport) {
    this.client = client;
    this.jsonSupport = jsonSupport;
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

  @Override public EventResource scope(String scope) {
    this.scope = scope;
    return this;
  }

  @Override public EventResource retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  @Override public EventResource flowId(final String flowId) {
    this.flowId = flowId;
    return this;
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

    if (collect.get(0).event() instanceof String) {
      return sendUsingSupplier(eventTypeName,
          () -> ("[" + Joiner.on(",").join(events) + "]").getBytes(Charsets.UTF_8));
    } else {
      return send(collect);
    }
  }

  @Override
  public <T> Response send(String eventTypeName, T event) {
    NakadiException.throwNonNull(eventTypeName, "Please provide an event type name");
    NakadiException.throwNonNull(event, "Please provide an event");

    if (event instanceof String) {
      return sendUsingSupplier(eventTypeName, () -> ("[" + event + "]").getBytes(Charsets.UTF_8));
    } else {
      ArrayList<T> events = new ArrayList<>(1);
      Collections.addAll(events, event);
      return send(eventTypeName, events);
    }
  }

  @Override public <T> BatchItemResponseCollection sendBatch(String eventTypeName, List<T> events) {
    Response send = send(eventTypeName, events);
    List<BatchItemResponse> items = Lists.newArrayList();

    if (send.statusCode() == 207 || send.statusCode() == 422) {
      try (ResponseBody responseBody = send.responseBody()) {
        items.addAll(jsonSupport.fromJson(responseBody.asReader(), TYPE_BIR));
      } catch (IOException e) {
        logger.error("Error handling BatchItemResponse " + e.getMessage(), e);
      }
    }

    return new BatchItemResponseCollection(items, LINKS_SENTINEL, client);
  }

  private Response sendUsingSupplier(String eventTypeName, ContentSupplier supplier) {
    // todo: close
    return timed(() -> client.resourceProvider()
                     .newResource()
                     .retryPolicy(retryPolicy)
                     .postEventsThrowing(
                         collectionUri(eventTypeName).buildString(), options(), supplier),
                 client,
                 1);
  }

  private <T> Response send(List<EventRecord<T>> events) {
    NakadiException.throwNonNull(events, "Please provide one or more event records");

    String topic = events.get(0).eventType();
    List<Object> eventList =
        events.stream().map(this::mapEventRecordToSerdes).collect(Collectors.toList());

    // todo: close
    return timed(() -> client.resourceProvider()
                     .newResource()
                     .retryPolicy(retryPolicy)
                     .postEventsThrowing(
                         collectionUri(topic).buildString(), options(), () -> jsonSupport.toJsonBytes(eventList)),
              client,
        eventList.size());
  }

  @VisibleForTesting <T> Object mapEventRecordToSerdes(EventRecord<T> er) {
    return jsonSupport.transformEventRecord(er);
  }

  private ResourceOptions options() {
      final ResourceOptions options = ResourceSupport.options(APPLICATION_JSON);
      options.tokenProvider(client.resourceTokenProvider());
      options.scope(applyScope(TokenProvider.NAKADI_EVENT_STREAM_WRITE));
      if (flowId != null) {
          options.flowId(flowId);
      }
      return options;
  }

  private UriBuilder collectionUri(String topic) {
    return UriBuilder.builder(client.baseURI())
        .path(PATH_EVENT_TYPES)
        .path(topic)
        .path(PATH_COLLECTION);
  }

  String applyScope(String fallbackScope) {
    return scope == null ? fallbackScope : scope;
  }
}
