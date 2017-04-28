package nakadi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Unstable
public class SubscriptionOffsetCheckpointer {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private final NakadiClient client;
  private boolean suppressingInvalidSessions;

  public SubscriptionOffsetCheckpointer(NakadiClient client) {
    this(client, false);
  }

  public SubscriptionOffsetCheckpointer(NakadiClient client, boolean suppressingInvalidSessions) {
    this.client = client;
    this.suppressingInvalidSessions = suppressingInvalidSessions;
  }

  /**
   * Ask the server to commit the supplied {@link StreamCursorContext} moving the subscription
   * offset to that value.
   *
   * @param context holds the cursor information.
   */
  public void checkpoint(StreamCursorContext context) {
    checkpoint(context, suppressingInvalidSessions);
  }

  /**
   * Ask the server to commit the supplied {@link StreamCursorContext} moving the subscription
   * offset to that point.
   *
   * @param context holds the cursor information.
   * @param suppressUnknownSessionError if true this will not throw 422 errors. See <a
   * href="https://github.com/zalando-incubator/nakadi-java/issues/117">issue 117</a> for details.
   */
  public void checkpoint(StreamCursorContext context, boolean suppressUnknownSessionError) {
    SubscriptionResource resource = client.resources().subscriptions();

    try {
      final CursorCommitResultCollection ccr = checkpoint(context, resource);

      if (!ccr.items().isEmpty()) {
        /*
       the cursor we  tried is older or equal to already committed one. A list of commit results
       is returned for this one.

       We could probably do something here based on a supplied policy, eg drop messages as
       they're being reconsumed until we catch up. For now don't do anything clever, just let
       the server absorb the commits and keep passing events along.
        */
        logger.info("subscription_checkpoint server_indicated_stale_cursor {}", ccr);
      } else {
        logger.info("subscription_checkpoint server_ok_accepted_cursor {}",
            cursorTrackingKey(context));
      }
    } catch (RateLimitException e) {
      /*
       * todo: we need to handle this, rethrow for now
       *
       * option most like is to go into a backoff and retry; we could wrap this up in an observable
       * and use the {@link StreamConnectionRetry}. atm we're on the computation rx thread
       * so we won't hold up raw consumption on the io scheduler if we spin but we will block
       * onNext processing in the stream processor which is also on the computation scheduler.
       * remember we have either 60s or max_uncommitted_events to make progress on the limit before
       * we get dropped by the server. if we do we have to spin for maybe another 60s to
       * reestablish the consumer connection.
       */
      logger.warn("subscription_checkpoint_err " + e.getMessage(), e);
      throw e;
    } catch (InvalidException e) {
      // todo: we need to get the server to send a specific problem, 422 is too broad
      if (suppressUnknownSessionError) {
        logger.info("suppressed_invalid_checkpoint_err {}", e.problem().title());
      } else {
        throw e;
      }
    } catch (NakadiException e) {
      throw e;
    } catch (Exception e) {
      throw new NakadiException(Problem.localProblem(e.getMessage(), ""), e);
    }
  }

  @VisibleForTesting
  CursorCommitResultCollection checkpoint(
      StreamCursorContext context, SubscriptionResource resource) {
    return resource.checkpoint(context.context(), context.cursor());
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
