package nakadi;

import java.lang.reflect.Type;
import java.util.HashMap;

class JsonBatchSupport {

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
    return jsonSupport.marshalEventStreamBatch(line, type);
  }
}
