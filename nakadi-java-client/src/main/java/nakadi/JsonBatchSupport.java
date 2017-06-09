package nakadi;

import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

class JsonBatchSupport {

  private static final Type EVENT_STREAM_BATCH_FIRSTPASS_TYPE =
      new TypeToken<EventStreamBatch<JsonObject>>() {
      }.getType();
  private final JsonSupport jsonSupport;

  public JsonBatchSupport(JsonSupport jsonSupport) {
    this.jsonSupport = jsonSupport;
  }

  public <T> StreamBatchRecord<T> lineToEventStreamBatchRecord(
      String line, Type eventType, StreamOffsetObserver observer) {
    return new StreamBatchRecordReal<>(marshalEventStreamBatch(line, eventType), observer);
  }

  public <T> StreamBatchRecord<T> lineToSubscriptionStreamBatchRecord(
      String line, Type type, StreamOffsetObserver observer, String xNakadiStreamId,
      String subscriptionId) {
    HashMap<String, String> context = new HashMap<>();
    context.put(StreamResourceSupport.X_NAKADI_STREAM_ID, xNakadiStreamId);
    context.put(StreamResourceSupport.SUBSCRIPTION_ID, subscriptionId);
    return new StreamBatchRecordReal<>(marshalEventStreamBatch(line, type), observer, context);
  }

  private <T> EventStreamBatch<T> marshalEventStreamBatch(String line, Type type) {
    // inefficient, marshal to map, then stringify JsonObject, then marshal to object :(
    EventStreamBatch<JsonObject> esb = marshalBatch(line, EVENT_STREAM_BATCH_FIRSTPASS_TYPE);
    List<T> ts = marshallEvents(type, esb.events());
    return new EventStreamBatch<>(esb.cursor(), esb.info(), ts);
  }

  private EventStreamBatch<JsonObject> marshalBatch(String line, Type type) {
    return jsonSupport.fromJson(line, type);
  }

  private <T> List<T> marshallEvents(Type type, List<JsonObject> events) {
    //noinspection unchecked
    return events.stream()
        // assumes the supplied type literal is of type T; if not this will throw a CCE
        .map(e -> jsonSupport.<T>marshalEvent(e.toString(), type)).collect(Collectors.toList());
  }
}
