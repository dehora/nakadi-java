package nakadi;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

class ResourceSupport {

  public static final String CHARSET_UTF_8 = "UTF-8";
  public static final String APPLICATION_JSON_CHARSET_UTF_8 =
      "application/json; charset=" + CHARSET_UTF_8;

  static String nextEid() {
    return UUID.randomUUID().toString();
  }

  static String nextFlowId() {
    return String.format("njc-%d-%016x", System.currentTimeMillis(),
        ThreadLocalRandom.current().nextLong());
  }

  public static ResourceOptions options(String accept) {
    return new ResourceOptions()
        .header(ResourceOptions.HEADER_ACCEPT, accept)
        .header(ResourceOptions.HEADER_ACCEPT_CHARSET, CHARSET_UTF_8)
        .header("User-Agent", NakadiClient.USER_AGENT)
        .flowId(ResourceSupport.nextFlowId());
  }

  public static ResourceOptions optionsWithJsonContent(ResourceOptions options) {
    return options.header(ResourceOptions.HEADER_CONTENT_TYPE, APPLICATION_JSON_CHARSET_UTF_8);
  }

}
