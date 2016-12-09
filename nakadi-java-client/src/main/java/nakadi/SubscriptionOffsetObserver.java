package nakadi;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

class SubscriptionOffsetObserver implements StreamOffsetObserver {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private final NakadiClient client;
  private Map<String, StreamCursorContext> lastCheckpointedMap = new HashMap<>();

  SubscriptionOffsetObserver(NakadiClient client) {
    this.client = client;
  }

  @Override public void onNext(StreamCursorContext context) {
    try {
      MDC.put("cursor_context", context.toString());
      logger.debug("subscription_checkpoint starting checkpoint {}", context);
      checkpoint(context);
    } finally {
      MDC.remove("cursor_context");
    }
  }

  private void checkpoint(StreamCursorContext context) {
    SubscriptionResource resource = client.resources().subscriptions();

    String cursorTrackingKey = cursorTrackingKey(context);

    if(lastCheckpointedMap.containsKey(cursorTrackingKey)) {
      logger.debug("subscription_checkpoint skipping was already checkpointed key={}",
          cursorTrackingKey);
      return;
    }

    //noinspection DanglingJavadoc,TryWithIdenticalCatches
    try {
      CursorCommitResultCollection ccr = resource.checkpoint(context.context(), context.cursor());

      if (!ccr.items().isEmpty()) {
        /*
       the cursor we  tried is older or equal to already committed one. A list of commit results
       is returned for this one.

       We could probably do something here based on a supplied policy, eg drop messages as
       they're being reconsumed until we catch up. For now don't do anything clever, just let
       the server absorb the commits and keep passing events along.
        */
        logger.debug("subscription_checkpoint stale cursor {}", ccr);
      } else {
        logger.debug("subscription_checkpoint ok {}", cursorTrackingKey);
        lastCheckpointedMap.put(cursorTrackingKey, context);
      }
    } catch (RateLimitException e) {
      /**
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
      logger.warn("subscription_checkpoint "+e.getMessage(), e);
      throw e;
    } catch (PreconditionFailedException e) {
      // eg bad cursors: Precondition Failed; offset 98 for partition 0 is unavailable (412)
      throw e;
    } catch (InvalidException e) {
      // eg Session with stream id 331bc59a-c4cb-4fc8-a63d-5946b0190340 not found
      throw e;
    } catch (NakadiException e) {
      throw e;
    } catch (Exception e) {
      throw new NakadiException(Problem.localProblem(e.getMessage(), ""), e);
    }
  }

  private String cursorTrackingKey(StreamCursorContext context) {
    return context.cursor().eventType().orElse("unknown-event-type")+"-"+context.cursor().partition()+"-"+context.cursor().offset();
  }
}
