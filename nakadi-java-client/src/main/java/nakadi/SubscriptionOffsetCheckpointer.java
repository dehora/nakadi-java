package nakadi;

import java.util.Objects;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Unstable
public class SubscriptionOffsetCheckpointer {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private final NakadiClient client;
  private volatile boolean suppressInvalidSessionException;
  private volatile boolean suppressNetworkException;
  private BiConsumer<CursorCommitResultCollection, StreamCursorContext> resultCollectionConsumer;

  public SubscriptionOffsetCheckpointer(NakadiClient client) {
    this.client = client;
    resultCollectionConsumer = (ccr, context) -> {
      if (ccr.items().isEmpty()) {
        // 204; the cursor was moved forward, the server doesn't return data
        logger.debug("subscription_checkpoint server_accepted_updated_cursor {}",
            cursorTrackingKey(context));
      } else {
        // 200; the cursor was older or equal to what's there, server sends back a non-empty array.
        logger.warn("subscription_checkpoint server_ok_indicated_stale_cursor {}", ccr);
      }
    };
  }

  /**
   * Allows a consumer to see the underlying response results from the server as a
   * {@link CursorCommitResultCollection} along with the initiating
   * {@link StreamCursorContext} for the batch.
   * <p>
   *  If the {@link CursorCommitResultCollection#items()} is empty this indicates the server
   *  accepted the request and moved the checkpoint forward. If the
   *  {@link CursorCommitResultCollection#items()} is non-empty this means one or more of the
   *  cursors sent the server was stale, which is strongly indicative of a processing problem.
   * </p>
   * <p>
   *  Exceptions thrown due to server issues or where the session id is stale (422 response code)
   *  will result in this consumer not being invoked. Exceptions thrown by this consumer are
   *  handled the same way as the configured {@link SubscriptionOffsetObserver}.
   * <p>
   *  Note that the consumer call will block this object's {@link #checkpoint} request and
   *  contributes to the overall batch processing time.
   * </p>
   *
   * @param resultCollectionConsumer a consumer that accepts the server's
   * {@link CursorCommitResultCollection} result and the batch's {@link StreamCursorContext}.
   * @return this
   */
  public SubscriptionOffsetCheckpointer withCursorCommitResultConsumer(
      BiConsumer<CursorCommitResultCollection, StreamCursorContext> resultCollectionConsumer) {
    NakadiException.throwNonNull(resultCollectionConsumer,
        "Please provide a non-null result consumer");
    this.resultCollectionConsumer = resultCollectionConsumer;
    return this;
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

  private void checkpointInner(
      StreamCursorContext context,
      boolean suppressInvalidSessionException,
      boolean suppressNetworkException) {
    SubscriptionResource resource = client.resources().subscriptions();

    try {
      final CursorCommitResultCollection ccr = checkpointInner(context, resource);

      if (ccr.items().isEmpty()) {
        client.metricCollector()
            .mark(MetricCollector.Meter.sessionCheckpointAcceptedCursor, 1);
      } else {
        client.metricCollector()
            .mark(MetricCollector.Meter.sessionCheckpointOkIndicatedStaleCursor, 1);
      }

      resultCollectionConsumer.accept(ccr, context);

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
        logger.info("suppressed_network_checkpoint_err {} {}", cursorTrackingKey(context), e.getMessage());
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
