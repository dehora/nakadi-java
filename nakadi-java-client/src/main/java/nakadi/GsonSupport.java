package nakadi;

import com.google.common.base.Charsets;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import java.io.Reader;
import java.lang.reflect.Type;
import java.time.OffsetDateTime;

class GsonSupport implements JsonSupport {

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

  /**
   * Punch  a hole in the abstraction to let us deal with business and undefined event types that
   * can't be marshalled sanely otherwise.
   */
  <T> T fromJson(JsonElement json, Class<T> classOfT) {
    return gson.fromJson(json, classOfT);
  }

  /**
   * Punch  a hole in the abstraction to let us deal with business and undefined event types that
   * can't be marshalled sanely otherwise.
   */
  <T> T fromJson(JsonElement json, Type t) {
    return gson.fromJson(json, t);
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
