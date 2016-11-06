package nakadi;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;

public class TestSupport {

  public static String load(String name) {
    try {
      return Resources.toString(Resources.getResource(name), Charsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static JsonSupport jsonSupport() {
    return new GsonSupport(); // todo: singleton holder?
  }

  public static NakadiClient newNakadiClient() {
    return newNakadiClient("http://example.org");
  }

  public static NakadiClient newNakadiClient(String baseURI) {
    return NakadiClient.newBuilder().baseURI(baseURI).build();
  }
}
