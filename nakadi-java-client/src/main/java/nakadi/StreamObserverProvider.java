package nakadi;

/**
 * Provides {@link StreamObserver} instances that accept batches of events from the stream. The
 * {@link StreamObserver} accepts {@link StreamBatchRecord} where each item in the batch has been
 * marshalled to an instance of T.
 *
 * The {@link StreamProcessorManaged} that produces the batches also needs to be configured with
 * a matching {@link TypeLiteral}. A mismatch between the configured {@link TypeLiteral}
 * and the parameterized type of this factory won't be caught on setup and will result in a
 * runtime error when the stream is consumed.
 *
 * @param <T> the type of the events in the batch
 */
public interface StreamObserverProvider<T> {

  /**
   * Supply a {@link StreamObserver} to the {@link StreamProcessorManaged}, that will be used
   * for processing batches.
   *
   * @return a new parameterized {@link StreamObserver}.
   */
  StreamObserver<T> createStreamObserver();

  /**
   * @return the captured generic type for consuming events.
   */
  TypeLiteral<T> typeLiteral();
}
