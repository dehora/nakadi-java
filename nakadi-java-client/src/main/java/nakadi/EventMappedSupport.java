package nakadi;

import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

class EventMappedSupport {

  private static final Type MAP_TYPE =
      new TypeToken<Map<String, Object>>() {
      }.getType();

  private static final String METADATA_FIELD = "metadata";
  private static final GsonSupport gson = new GsonSupport();

  static Object mapEventRecordToSerdes(EventRecord<? extends Event> eventRecord) {

    if (eventRecord.event().getClass().isAssignableFrom(BusinessEventMapped.class)) {

      BusinessEventMapped businessEvent = (BusinessEventMapped) eventRecord.event();
      Map<String, Object> jsonEvent = Maps.newHashMap();
      jsonEvent.put("metadata", businessEvent.metadata());
      /*
      :hack: take the businessEvent.data field whose type we don't know and roundtrip it to
      a JSON string and back into to a regular map, so we can place that resulting map's fields
      into the top level of the jsonEvent. The result is an event that gets published to
      Nakadi whose fields are all in the top level JSON doc as per the the business category
      definition.
       */
      jsonEvent.putAll(gson.fromJson(gson.toJson(businessEvent.data()), MAP_TYPE));
      return jsonEvent;
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
    // undefined & business are non-generic; fingers crossed rawType is enough to check here.
    return rawType.isAssignableFrom(c);
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
