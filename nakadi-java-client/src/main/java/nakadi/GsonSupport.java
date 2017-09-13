package nakadi;

import com.google.common.base.Charsets;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.io.Reader;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class GsonSupport implements JsonSupport {

  private static final Type EVENT_STREAM_BATCH_FIRSTPASS_TYPE =
      new TypeToken<EventStreamBatch<JsonObject>>() {
      }.getType();

  static <T> boolean isAssignableFrom(Type type, Class<? super T> c) {
    TypeToken<T> typeToken = (TypeToken<T>) TypeToken.get(type);
    Class<? super T> rawType = typeToken.getRawType();
    return c.isAssignableFrom(rawType);
  }

  private static final String METADATA_FIELD = "metadata";

  private static final Type OFFSET_DATE_TIME_TYPE = new TypeToken<OffsetDateTime>() {
  }.getType();

  private final Gson gson;
  private final Gson gsonCompressed;

  public GsonSupport() {
    gson = gson();
    gsonCompressed = gsonCompressed();
  }

  public static Gson gsonCompressed() {
    return GsonCompressedHolder.INSTANCE;
  }

  public static Gson gson() {
    return GsonHolder.INSTANCE;
  }

  @Override public String toJsonCompressed(Object o) {
    return gsonCompressed.toJson(o);
  }

  @Override public String toJson(Object o) {
    return gson.toJson(o);
  }

  @Override public byte[] toJsonBytes(Object o) {
    return toJson(o).getBytes(Charsets.UTF_8);
  }

  @Override public byte[] toJsonBytesCompressed(Object o) {
    return toJsonCompressed(o).getBytes(Charsets.UTF_8);
  }

  @Override public <T> T fromJson(String raw, Class<T> c) {
    if (String.class.isAssignableFrom(c)) {
      //noinspection unchecked
      return (T) raw;
    }
    return gson.fromJson(raw, c);
  }

  public <T> T fromJson(String raw, Type tType) {
    if (tType.getTypeName().equals("java.lang.String")) {
      //noinspection unchecked
      return (T) raw;
    }
    return gson.fromJson(raw, tType);
  }

  @Override public <T> T fromJson(Reader r, Class<T> c) {
    return gson.fromJson(r, c);
  }

  @Override public <T> T fromJson(Reader r, Type tType) {
    return gson.fromJson(r, tType);
  }

  @Override public <T> Object transformEventRecord(EventRecord<T> eventRecord) {

    if (eventRecord.event().getClass().isAssignableFrom(BusinessEventMapped.class)) {

      BusinessEventMapped businessEvent = (BusinessEventMapped) eventRecord.event();
      /*
      :hack: take the businessEvent.data field whose type we don't know and build up a
      JSON object merging the businessEvent.data fields with a "metadata" field. The result is
      an event that gets published to Nakadi whose fields are all in the top level JSON doc as
      per the the business category definition.
       */
      final Gson gson = GsonSupport.gson();
      final JsonObject jsonObject = new JsonObject();
      jsonObject.add("metadata", gson.toJsonTree(businessEvent.metadata()));
      final JsonElement jsonElement = gson.toJsonTree(businessEvent.data());
      for (Map.Entry<String, JsonElement> entry : jsonElement.getAsJsonObject().entrySet()) {
        jsonObject.add(entry.getKey(), entry.getValue());
      }
      return jsonObject;
    }

    if (eventRecord.event().getClass().isAssignableFrom(UndefinedEventMapped.class)) {
      UndefinedEventMapped mapped = (UndefinedEventMapped) eventRecord.event();
      return mapped.data();
    }

    return eventRecord.event();
  }

  private <T> UndefinedEventMapped<T> marshalUndefinedEventMapped(String raw, Type type) {

    if (!isAssignableFrom(type, UndefinedEventMapped.class)) {
      throw new IllegalArgumentException(
          "Supplied type must be assignable from UndefinedEventMapped " + type.getTypeName());
    }

    if (type instanceof ParameterizedType) {
      /*
      we want the generic parameter type of T captured by UndefinedEventMapped<T> as that's
      what'll we deser the json with before setting it into UndefinedEventMapped.data. This is
      expected to work as well for a T that is itself carrying a generic or a generic collection.
      See EventMappedSupportTest.
      */
      ParameterizedType genericType = (ParameterizedType) type;
      Type[] actualTypeArguments = genericType.getActualTypeArguments();
      Type serdeType = actualTypeArguments[0];
      T data = fromJson(raw, serdeType);
      return new UndefinedEventMapped<>(data);
    } else {
      throw new IllegalArgumentException(
          "Supplied type must be a parameterized UndefinedEventMapped"
              + type.getTypeName());
    }
  }

  @VisibleForTesting
  <T> BusinessEventMapped<T> marshalBusinessEventMapped(String raw, Type type) {


    if (!isAssignableFrom(type, BusinessEventMapped.class)) {
      throw new IllegalArgumentException(
          "Supplied type must be assignable BusinessEventMapped " + type.getTypeName());
    }

    JsonObject jo = fromJson(raw, JsonObject.class);

    // pluck out the metadata block and marshal it
    EventMetadata metadata = gson.fromJson(jo.remove(METADATA_FIELD), EventMetadata.class);

    if (type instanceof ParameterizedType) {
      /*
      we want the generic parameter type of T captured by BusinessEventMapped<T> as that's
      what'll we deser the json with before setting it into BusinessEventMapped.data. This is
      expected to work as well for a T that is itself carrying a generic or a generic collection.
      See EventMappedSupportTest.
     */
      ParameterizedType genericType = (ParameterizedType) type;
      Type[] actualTypeArguments = genericType.getActualTypeArguments();
      Type serdeType = actualTypeArguments[0];
      T data = gson.fromJson(jo, serdeType);
      return new BusinessEventMapped<>(data, metadata);
    } else {
      throw new IllegalArgumentException(
          "Supplied type must be a parameterized BusinessEventMapped"
              + type.getTypeName());
    }
  }

  private <T> T marshalEvent(String raw, Type type) {
    /*
     * Herein some workarounds to handle business and undefined event types. Those two are
     * defined in the API to be extended/subclassed by custom schema, but have no extension
     * point in their API definitions to hold the custom data part of the event
     * (whereas a datachange event does have a holder field called 'data' that we can get
     * access to at runtime).
     *
     * This means standard code or hand generated implementations of those categories will
     * drop the custom data on the floor as there's no fields to marshall the data into.
     * You are left then with the options of exporting a non-domain option such as given
     * library's json tree structure, raw strings, bytes or maps.
     *
     * Most users will just pass a generic or string option instead of actually using
     * undefined or business event types given this limitation. But the aim of this client
     * is to provide a complete implementation of the api, so we do a bit of extra work
     * here to support the two categories.
     *
     * We look at the Type passed in and if it's one of the two categories we ask
     *  to  deserialize it remapping the custom fields into
     * the data field for those categories.
     */

    T t;

    if (isAssignableFrom(type, UndefinedEventMapped.class)) {
      //noinspection unchecked
      t = (T) marshalUndefinedEventMapped(raw, type);
    } else if (isAssignableFrom(type, BusinessEventMapped.class)) {
      //noinspection unchecked
      t = (T) marshalBusinessEventMapped(raw, type);
    } else {
      t = fromJson(raw, type);
    }

    return t;
  }

  @Override public <T> EventStreamBatch<T> marshalEventStreamBatch(String raw, Type type) {
    // inefficient, marshal to map, then stringify JsonObject, then marshal to object :(
    EventStreamBatch<JsonObject> esb = marshalBatch(raw, EVENT_STREAM_BATCH_FIRSTPASS_TYPE);
    List<T> ts = marshallEvents(type, esb.events());
    return new EventStreamBatch<>(esb.cursor(), esb.info(), ts);
  }

  private EventStreamBatch<JsonObject> marshalBatch(String line, Type type) {
    return fromJson(line, type);
  }

  private <T> List<T> marshallEvents(Type type, List<JsonObject> events) {
    //noinspection unchecked
    return events.stream()
        // assumes the supplied type literal is of type T; if not this will throw a CCE
        .map(e -> this.<T>marshalEvent(e.toString(), type)).collect(Collectors.toList());
  }

  private static class GsonCompressedHolder {

    private static final Gson INSTANCE = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .registerTypeAdapter(OFFSET_DATE_TIME_TYPE, new OffsetDateTimeSerdes())
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        .create();
  }

  private static class GsonHolder {

    private static final Gson INSTANCE = new GsonBuilder()
        .setPrettyPrinting()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .registerTypeAdapter(OFFSET_DATE_TIME_TYPE, new OffsetDateTimeSerdes())
        //todo: test utc-ness of this, cf https://github.com/google/gson/issues/281
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        .create();
  }
}
