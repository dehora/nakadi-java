package nakadi;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Unstable
public class SubscriptionOffsetCheckpointer {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private final NakadiClient client;
  private volatile boolean suppressInvalidSessionException;
  private volatile boolean suppressNetworkException;

  public SubscriptionOffsetCheckpointer(NakadiClient client) {
    this.client = client;
  }

  /**
   * This is deprecated and will be removed in 0.9.0. Prefer setting the
   * {@link #suppressInvalidSessionException} method instead.
   *
   * @param client the client
   * @param suppressingInvalidSessions whether or not to throw invalid session responses
   */
  @Deprecated
  public SubscriptionOffsetCheckpointer(
      NakadiClient client, @Deprecated boolean suppressingInvalidSessions) {
    this.client = client;
    this.suppressInvalidSessionException = suppressingInvalidSessions;
  }

  /**
   * If true tells the checkpointer to suppress invalid session responses from the server instead
   * of throwing {@link NetworkException}.
   *
   * @param suppressInvalidSessions whether or not to throw invalid session responses
   * @return this
   */
  public SubscriptionOffsetCheckpointer suppressInvalidSessionException(
      boolean suppressInvalidSessions) {
    this.suppressInvalidSessionException = suppressInvalidSessions;
    return this;
  }

  /**
   * If true tells the checkpointer to suppress network level errors via the server instead
   * of throwing {@link InvalidException}.
   *
   * @param suppressNetworkException whether or not to throw network level exceptions
   * @return this
   */
  public SubscriptionOffsetCheckpointer suppressNetworkException(boolean suppressNetworkException) {
    this.suppressNetworkException = suppressNetworkException;
    return this;
  }

  /**
   * Ask the server to commit the supplied {@link StreamCursorContext} moving the subscription
   * offset to that value.
   *
   * @param context holds the cursor information.
   */
  public void checkpoint(StreamCursorContext context) {
    checkpointInner(context, suppressInvalidSessionException, suppressNetworkException);
  }

  /**
   * Ask the server to commit the supplied {@link StreamCursorContext} moving the subscription
   * offset to that point.
   * <p>
   * This is deprecated and will be removed in 0.9.0. Prefer setting the
   * {@link #suppressInvalidSessionException} method instead and calling
   * {@link #checkpoint(StreamCursorContext)}.
   * </p>
   *
   * @param context holds the cursor information.
   * @param suppressUnknownSessionError if true this will not throw 422 errors. See <a
   * href="https://github.com/zalando-incubator/nakadi-java/issues/117">issue 117</a> for details.
   */
  @Deprecated
  public void checkpoint(StreamCursorContext context, boolean suppressUnknownSessionError) {
    checkpointInner(context, suppressUnknownSessionError, suppressNetworkException);
  }

  private void checkpointInner(
      StreamCursorContext context,
      boolean suppressInvalidSessionException,
      boolean suppressNetworkException) {
    SubscriptionResource resource = client.resources().subscriptions();

    try {
      final CursorCommitResultCollection ccr = checkpointInner(context, resource);

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
       * and use the {@link RequestRetry}. atm we're on the computation rx thread
       * so we won't hold up raw consumption on the io scheduler if we spin but we will block
       * onNext processing in the stream processor which is also on the computation scheduler.
       * remember we have either 60s or max_uncommitted_events to make progress on the limit before
       * we get dropped by the server. if we do we have to spin for maybe another 60s to
       * reestablish the consumer connection.
       */
      logger.warn("subscription_checkpoint_err " + e.getMessage(), e);
      throw e;
    } catch (InvalidException e) {
      // todo: waiting on https://github.com/zalando/nakadi/issues/651 to provide a specific error
      client.metricCollector().mark(MetricCollector.Meter.sessionCheckpointMismatch, 1);
      if (suppressInvalidSessionException) {
        logger.info("suppressed_invalid_checkpoint_err {}", e.getMessage());
      } else {
        throw e;
      }
    } catch (NetworkException e) {
      client.metricCollector().mark(MetricCollector.Meter.sessionCheckpointNetworkException, 1);
      if (suppressNetworkException) {
        logger.info("suppressed_network_checkpoint_err {}", e.getMessage());
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
  CursorCommitResultCollection checkpointInner(
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

  @VisibleForTesting
  boolean suppressInvalidSessionException() {
    return suppressInvalidSessionException;
  }

  @VisibleForTesting
  boolean suppressNetworkException() {
    return suppressNetworkException;
  }

  @Override public int hashCode() {
    return Objects.hash(client, suppressInvalidSessionException, suppressNetworkException);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubscriptionOffsetCheckpointer that = (SubscriptionOffsetCheckpointer) o;
    return suppressInvalidSessionException == that.suppressInvalidSessionException &&
        suppressNetworkException == that.suppressNetworkException &&
        Objects.equals(client, that.client);
  }

  @Override public String toString() {
    return "SubscriptionOffsetCheckpointer{" + "client=" + client +
        ", suppressInvalidSessionException=" + suppressInvalidSessionException +
        ", suppressNetworkException=" + suppressNetworkException +
        '}';
  }
}
