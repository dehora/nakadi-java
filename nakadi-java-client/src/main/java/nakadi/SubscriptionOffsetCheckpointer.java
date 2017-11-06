package nakadi;

import java.util.Objects;

@Unstable
public class SubscriptionOffsetCheckpointer implements Checkpointer {

  private final NakadiClient client;
  private volatile boolean suppressInvalidSessionException;
  private volatile boolean suppressNetworkException;

  private SubscriptionObservableResultCheckpointer delegate;

  public SubscriptionOffsetCheckpointer(NakadiClient client) {
    this.client = client;
    delegate = new SubscriptionObservableResultCheckpointer(client)
        /*
         these are suppressed by default in delegate, set them to true here to honor this
         object's default contract, which is unsuppressed.
          */
        .suppressNetworkException(false)
        .suppressInvalidSessionException(false);
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
    delegate.checkpoint(context);
  }

  @VisibleForTesting
  CursorCommitResultCollection checkpointInner(
      StreamCursorContext context, SubscriptionResource resource) {
    return resource.checkpoint(context.context(), context.cursor());
  }

  @VisibleForTesting
  boolean suppressInvalidSessionException() {
    return suppressInvalidSessionException;
  }

  @VisibleForTesting
  boolean suppressNetworkException() {
    return suppressNetworkException;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubscriptionOffsetCheckpointer that = (SubscriptionOffsetCheckpointer) o;
    return suppressInvalidSessionException == that.suppressInvalidSessionException &&
        suppressNetworkException == that.suppressNetworkException &&
        Objects.equals(client, that.client) &&
        Objects.equals(delegate, that.delegate);
  }

  @Override public int hashCode() {
    return Objects.hash(client, suppressInvalidSessionException, suppressNetworkException,
        delegate);
  }

  @Override public String toString() {
    return "SubscriptionOffsetCheckpointer{" + "client=" + client +
        ", suppressInvalidSessionException=" + suppressInvalidSessionException +
        ", suppressNetworkException=" + suppressNetworkException +
        ", delegate=" + delegate +
        '}';
  }
}
