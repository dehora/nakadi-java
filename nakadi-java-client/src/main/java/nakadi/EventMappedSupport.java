package nakadi;

import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Map;

class EventMappedSupport {

  private static final Type MAP_TYPE =
      new TypeToken<Map<String, Object>>() {
      }.getType();

  private static final String METADATA_FIELD = "metadata";
  private static final GsonSupport gson = new GsonSupport();
  private static TypeLiteral<BusinessEventMapped> businessEventMappedTypeLiteral =
      new TypeLiteral<BusinessEventMapped>() {
      };
  private static TypeLiteral<UndefinedEventMapped> undefinedEventMappedTypeLiteral =
      new TypeLiteral<UndefinedEventMapped>() {
      };

  static Object mapEventRecordToSerdes(EventRecord<? extends Event> eventRecord) {

    if (eventRecord.event().getClass().isAssignableFrom(BusinessEventMapped.class)) {
      BusinessEventMapped mapped = (BusinessEventMapped) eventRecord.event();
      Map<String, Object> data = Maps.newHashMap();
      data.put("metadata", mapped.metadata());
      data.putAll(mapped.data());
      return data;
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

  static UndefinedEventMapped marshalUndefinedEventMapped(Map<String, Object> data) {
    return new UndefinedEventMapped().data(data);
  }

  static BusinessEventMapped marshalBusinessEventMapped(String raw, JsonSupport jsonSupport) {
    JsonObject jo = jsonSupport.fromJson(raw, JsonObject.class);

    // pluck out the metadata block and marshal it
    EventMetadata metadata = gson.fromJson(jo.remove(METADATA_FIELD), EventMetadata.class);

    // grab the remaining custom data and stuff it into a map
    Map<String, Object> custom = gson.fromJson(jo, MAP_TYPE);

    //noinspection unchecked
    return new BusinessEventMapped().data(custom).metadata(metadata);
  }
}
