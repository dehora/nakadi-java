package nakadi;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

class ResourceSupport {

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
        .header("Accept-Charset", "UTF-8")
        .header("User-Agent", NakadiClient.USER_AGENT)
        .header("X-Client-Platform-Details", NakadiClient.PLATFORM_DETAILS_JSON)
        .flowId(ResourceSupport.nextFlowId());
  }
}
