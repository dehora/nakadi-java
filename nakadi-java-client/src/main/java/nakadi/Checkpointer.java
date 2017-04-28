package nakadi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Checkpointer {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private NakadiClient client;

  public Checkpointer(NakadiClient client) {
    this.client = client;
  }

  public void checkpoint(StreamCursorContext context) {
    checkpoint(context, false);
  }

  public void checkpoint(StreamCursorContext context, boolean suppressUnknownSession) {
    SubscriptionResource resource = client.resources().subscriptions();
    String cursorTrackingKey = cursorTrackingKey(context);
    try {
      CursorCommitResultCollection ccr = resource.checkpoint(context.context(), context.cursor());
      if (!ccr.items().isEmpty()) {
        logger.info("subscription_checkpoint server_indicated_stale_cursor {}", ccr);
      } else {
        logger.info("subscription_checkpoint server_ok_accepted_cursor {}", cursorTrackingKey);
      }
    } catch (InvalidException e) {
      if (suppressUnknownSession) {
        logger.info("subscription_checkpoint_invalid_suppressed " + e.getMessage());
      } else {
        logger.warn("subscription_checkpoint_invalid " + e.getMessage());
        throw e;
      }
    } catch (NakadiException e) {
      throw e;
    } catch (Exception e) {
      throw new NakadiException(Problem.localProblem(e.getMessage(), ""), e);
    }
  }

  private String cursorTrackingKey(StreamCursorContext context) {
    Cursor cursor = context.cursor();
    if (cursor != null) {
      String partition = cursor.partition();
      String offset = cursor.offset();
      return cursor.eventType().orElse("unknown-event-type") + "-" + partition + "-" + offset;
    } else {
      logger.warn("unexpected empty cursor {}", context);
      return context.toString();
    }
  }
}
