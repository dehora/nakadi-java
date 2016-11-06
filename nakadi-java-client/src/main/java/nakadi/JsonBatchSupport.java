package nakadi;

import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class JsonBatchSupport {

  public static final String METADATA_FIELD = "metadata";
  private static final Type EVENT_STREAM_BATCH_FIRSTPASS_TYPE =
      new TypeToken<EventStreamBatch<JsonObject>>() {
      }.getType();
  private static final Type MAP_TYPE =
      new TypeToken<Map<String, Object>>() {
      }.getType();
  private final JsonSupport jsonSupport;
  private final GsonSupport gson;

  public JsonBatchSupport(JsonSupport jsonSupport) {
    this.jsonSupport = jsonSupport;
    gson = new GsonSupport();
  }

  public <T> StreamBatchRecord<T> lineToEventStreamBatchRecord(
      String line, Type eventType, StreamOffsetObserver observer) {
    return new StreamBatchRecordReal<>(marshalEventStreamBatch(line, eventType), observer);
  }

  public <T> StreamBatchRecord<T> lineToSubscriptionStreamBatchRecord(
      String line, Type type, StreamOffsetObserver observer, String xNakadiStreamId,
      String subscriptionId) {
    HashMap<String, String> context = Maps.newHashMap();
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
        .map(e -> {
          T t;
          /*
           * Herein some workarounds to handle business and undefined event types. Those two are
           * defined in the API to be extended/subclassed by custom schema but have no extension
           * point in their API definitions to hold the custom data that's custom to the event
           * (whereas a datachange event does have a holder field called 'data' that we can get
           * access to at runtime). This means standard code or hand generated implementations
           * of those categories will drop the custom data on the floor as there's no fields
           * to marshall the data into. You are left then with the options of exporting a
           * given library's json data structure, raw strings, or maps.
           *
           * Most users will just pass a generic or string option instead of undefined or
           * business event types. But the aim of this client is to provide a complete
           * implementation of the api, so we do a bit of work here for that.
           *
           * We look at the Type passed in and use gson's TypeToken to get at the underlying
           * class the type is carrying (remembering that it was created via a TypeLiteral,
           * client users can't pass in Type fields directly). If it's an UndefinedEventMapped or
           * a BusinessEventMapped try and remap the custom fields into the synthetic 'data' holder
           * each of those objects has. We access gson directly and break the abstraction in
           * this case but its builtins are very well tested.
           */
          // undefined & business are non-generic; fingers crossed rawType is enough to check here.
          if (EventMappedSupport.isAssignableFrom(type, UndefinedEventMapped.class)) {
            Map<String, Object> data = jsonSupport.fromJson(e.toString(), MAP_TYPE);
            //noinspection unchecked
            t = (T) EventMappedSupport.marshalUndefinedEventMapped(data);
          } else if (EventMappedSupport.isAssignableFrom(type, BusinessEventMapped.class)) {
            //noinspection unchecked
            t = (T) EventMappedSupport.marshalBusinessEventMapped(e.toString(), jsonSupport);
          } else {
            t = jsonSupport.fromJson(e.toString(), type);
          }

          return t;
        }).collect(Collectors.toList());
  }
}
