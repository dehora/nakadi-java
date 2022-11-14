package nakadi;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

class ResourceSupport {

  public static final String CHARSET_UTF_8 = "UTF-8";
  public static final String APPLICATION_JSON_CHARSET_UTF_8 = "application/json; charset=utf8";

  static String nextEid() {
    return UUID.randomUUID().toString();
  }

  static String nextFlowId() {
    return String.format("njc-%d-%016x", System.currentTimeMillis(),
        ThreadLocalRandom.current().nextLong());
  }

  public static ResourceOptions options(String accept) {
    return new ResourceOptions()
        .header("Accept", accept)
        .header("Accept-Charset", CHARSET_UTF_8)
        .header("User-Agent", NakadiClient.USER_AGENT)
        .flowId(ResourceSupport.nextFlowId());
  }

  public static ResourceOptions optionsWithJsonContent(ResourceOptions options) {
    return options.header("Content-Type", APPLICATION_JSON_CHARSET_UTF_8);
  }

}
