package nakadi.json.gson;

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
import java.time.temporal.TemporalAccessor;

class OffsetDateTimeSerdes
    implements JsonDeserializer<OffsetDateTime>, JsonSerializer<OffsetDateTime> {

  private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

  @Override public JsonElement serialize(OffsetDateTime src, Type typeOfSrc,
      JsonSerializationContext context) {
    return new JsonPrimitive(ISO.format(src));
  }

  @Override public OffsetDateTime deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    return toOffsetDateTime(json.getAsString());
  }

  OffsetDateTime toOffsetDateTime(String raw) {
    if (raw.contains(":59:60")) {
      TemporalAccessor parse = DateTimeFormatter.ISO_INSTANT.parse(raw);
      if (parse.query(DateTimeFormatter.parsedLeapSecond())) {
        return ISO.parse(raw.replace(":59:60", ":59:59"), OffsetDateTime::from);
      }
    }
    return ISO.parse(raw, OffsetDateTime::from);
  }

}
