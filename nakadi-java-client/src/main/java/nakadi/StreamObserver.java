package nakadi;

import java.util.Optional;

/**
 * Observer for batches of events.
 *
 * <p>The {@link StreamProcessor} will call this observer. A mismatch between the configured
 * {@link TypeLiteral} and the parameterized type of this factory won't be caught on setup
 * and will result in a runtime error when the stream is being prepared for the observer.</p>
 *
 * @param <T> the type of the events in the batch
 */
public interface StreamObserver<T> {

  /**
   * Called before stream connection begins and every time a retry is made.
   * Implementations can set up here.
   */
  void onStart();

  /**
   * Called after the stream either cannot be recovered and retried, or has completed.
   */
  void onStop();

  /**
   * Notifies the Observer that the {@link StreamProcessorManaged} has finished sending batches.
   *
   * <p>Once the {@link StreamProcessorManaged} calls this method, it will not call
   * {@link #onNext}. </p>
   */
  void onCompleted();

  /**
   * Notifies the Observer that the {@link StreamProcessorManaged} has seen an error.
   *
   * <p>
   *  By the time this is called the processor will not attempt to retry further. The argument is
   *  effectively a message for the observer.
   * </p>
   * <p>
   *  Note: the argument may be instance of an {@link Error}.
   * </p>
   *
   * @param t the exception sent by the {@link StreamProcessor}
   */
  void onError(Throwable t);

  /**
   * Provides the Observer with a new {@link StreamBatchRecord}.
   *
   * <p>The {@link StreamProcessor} may call this method 0 to many times.</p>
   *
   * <p>
   * Exceptions thrown from this method will generally not stop the {@link StreamProcessor} from
   * entering into a connection retry (an {@link Error} is not considered recoverable and  will
   * result in the {@link StreamProcessor} stopping). Implementations that want to force the
   * underlying {@link StreamProcessor} to stop should throw {@link NonRetryableNakadiException}.
   * In that case the {@link StreamProcessor} will stop and not reconnect or retry further.
   * </p>
   *
   * <p>The {@link StreamProcessor} will not call this method again once it calls either
   * {@link #onCompleted} or {@link #onError} before entering its retry loop.</p>
   *
   * @param record the emitted {@link StreamBatchRecord}
   */
  void onNext(StreamBatchRecord<T> record);

  /**
   * A backpressure control to request a certain maximum number of emitted items from the
   * {@link StreamProcessor}. This is called per emission, and clients may dynamically change
   * the value.
   *
   * <p>To disable backpressure, return {@link Optional#empty()}.</p>
   *
   * @return the maximum number of items you want the {@link StreamProcessor} to emit to the at this
   * time and {@link Optional#empty()} to let {@link StreamProcessor} control emitted items.
   */
  Optional<Long> requestBackPressure();

  /**
   * Ask the {@link StreamProcessor} to buffer batches before emitting them. This is called
   * on initialization and fixed thereafter.
   *
   * @return the size of the requestBuffer or {@link Optional#empty()} to indicate no buffering
   */
  Optional<Integer> requestBuffer();
}
