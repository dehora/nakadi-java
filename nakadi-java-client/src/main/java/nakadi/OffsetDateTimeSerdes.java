package nakadi;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

class OffsetDateTimeSerdes
    implements JsonDeserializer<OffsetDateTime>, JsonSerializer<OffsetDateTime> {

  private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

  @Override public JsonElement serialize(OffsetDateTime src, Type typeOfSrc,
      JsonSerializationContext context) {
    return new JsonPrimitive(fromOffsetDateTime(src));
  }

  @Override public OffsetDateTime deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    final String raw = json.getAsString();
    return toOffsetDateTime(raw);
  }

  String fromOffsetDateTime(OffsetDateTime src) {
    return ISO.format(src);
  }

  OffsetDateTime toOffsetDateTime(String raw) {
    return ISO.parse(raw, OffsetDateTime::from);
  }
}
