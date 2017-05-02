package nakadi;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class EventMappedSupport {

  private static final Type MAP_TYPE =
      new TypeToken<Map<String, Object>>() {
      }.getType();

  private static final String METADATA_FIELD = "metadata";
  private static final GsonSupport gson = new GsonSupport();

  static <T> Object mapEventRecordToSerdes(EventRecord<T> eventRecord) {

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

  static <T> boolean isAssignableFrom(Type type, Class<? super T> c) {
    TypeToken<T> typeToken = (TypeToken<T>) TypeToken.get(type);
    Class<? super T> rawType = typeToken.getRawType();
    return c.isAssignableFrom(rawType);
  }

  static <T> UndefinedEventMapped<T> marshalUndefinedEventMapped(String raw, Type type,
      JsonSupport jsonSupport) {

    if (!EventMappedSupport.isAssignableFrom(type, UndefinedEventMapped.class)) {
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
      T data = jsonSupport.fromJson(raw, serdeType);
      return new UndefinedEventMapped<>(data);
    } else {
      throw new IllegalArgumentException(
          "Supplied type must be a parameterized UndefinedEventMapped"
              + type.getTypeName());
    }
  }

  static <T> BusinessEventMapped<T> marshalBusinessEventMapped(String raw, Type type, JsonSupport jsonSupport) {

    if (!EventMappedSupport.isAssignableFrom(type, BusinessEventMapped.class)) {
      throw new IllegalArgumentException(
          "Supplied type must be assignable BusinessEventMapped " + type.getTypeName());
    }

    JsonObject jo = jsonSupport.fromJson(raw, JsonObject.class);

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
}
