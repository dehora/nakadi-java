package nakadi;

/**
 * Supplies a {@link StreamBatch} and {@link StreamOffsetObserver} to
 * the {@link StreamObserver}.
 *
 * @param <T> the type of the events in the batch
 */
public interface StreamBatchRecord<T> {

  /**
   * The batch of events emitted by the {@link StreamProcessorManaged}.
   *
   * @return a parameterized {@link StreamBatch}
   */
  StreamBatch<T> streamBatch();

  /**
   * Contains the {@link Cursor} and extra context from a subscription stream batch. The
   * {@link Cursor} is the same one associated with the {@link StreamBatch}.
   *
   * @return the {@link StreamCursorContext} for this batch.
   */
  StreamCursorContext streamCursorContext();

  /**
   * An observer that can be called by {@link StreamObserver} when it has completed processing
   * the batch.
   * <p>
   * Callers using a subscription stream connection can rely on the default implementation provided
   * to checkpoint with the server when {@link StreamOffsetObserver#onNext(StreamCursorContext)} is
   * called.</p>
   *
   * @return the {@link StreamOffsetObserver}
   */
  StreamOffsetObserver streamOffsetObserver();
}
