package nakadi;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OffsetDateTimeSerdes
    implements JsonDeserializer<OffsetDateTime>, JsonSerializer<OffsetDateTime> {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());
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
    return toDateObject(raw, OffsetDateTime::from);
  }

  Instant toInstant(String raw) {
    return toDateObject(raw, Instant::from);
  }

  private <T> T toDateObject(String raw, TemporalQuery<T> from) {
    // lo-fi detect a leap
    if (raw.contains(":59:60")) {
      // ISO_INSTANT doesn't crash on leap seconds
      TemporalAccessor parse = DateTimeFormatter.ISO_INSTANT.parse(raw);
      if (parse.query(DateTimeFormatter.parsedLeapSecond())) {
        // lazy: push back the second instead of bump to tomm requires less time manipulation
        logger.warn("saw leap second, shifting the date back 1s, {}", raw);
        return ISO.parse(raw.replace(":59:60", ":59:59"), from);
      }
    }
    return ISO.parse(raw, from);
  }
}
