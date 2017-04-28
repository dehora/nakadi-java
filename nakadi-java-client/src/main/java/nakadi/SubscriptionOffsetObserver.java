package nakadi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

class SubscriptionOffsetObserver implements StreamOffsetObserver {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private final NakadiClient client;

  SubscriptionOffsetObserver(NakadiClient client) {
    this.client = client;
  }

  @Override public void onNext(StreamCursorContext context) {
    try {
      MDC.put("cursor_context", context.toString());
      logger.debug("subscription_checkpoint starting checkpoint {}", context);
      new SubscriptionOffsetCheckpointer(client).checkpoint(context);
    } finally {
      MDC.remove("cursor_context");
    }
  }
}
