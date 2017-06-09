package nakadi;

import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;

class EventMappedSupport {


  static <T> boolean isAssignableFrom(Type type, Class<? super T> c) {
    TypeToken<T> typeToken = (TypeToken<T>) TypeToken.get(type);
    Class<? super T> rawType = typeToken.getRawType();
    return c.isAssignableFrom(rawType);
  }
}
