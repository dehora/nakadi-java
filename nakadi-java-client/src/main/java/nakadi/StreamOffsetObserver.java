package nakadi;

/**
 * Can be called by the {@link StreamObserver} to indicate a batch has been processed.
 *
 * Typically this is used to implement a checkpointer that can store the position of the
 * client in the stream to track their progress.
 */
public interface StreamOffsetObserver {

  /**
   * Receives a {@link StreamCursorContext} that can be used to checkpoint (or more generally,
   * observe) progress in the stream.
   * <p></p>
   * The default observer for a subscription based {@link StreamProcessor} (one which has been
   * given a subscription id via {@link StreamConfiguration#subscriptionId} is
   * {@link SubscriptionOffsetObserver}. This will checkpoint back to the server each time
   * it's called. This behaviour can be replaced by supplying a different checkpointer via
   * {@link StreamProcessor.Builder#streamOffsetObserver}.
   *
   * @param streamCursorContext the batch's {@link StreamCursorContext}.
   *
   * todo: see if we need to declare checked exceptions here to force the observer to handle.
   */
  void onNext(StreamCursorContext streamCursorContext) throws NakadiException;
}
