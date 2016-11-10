package nakadi;

import java.util.Optional;

/**
 * Interim class that provides no-ops for request backpresssure and buffering.
 *
 * @param <T> the type of the observer
 */
abstract public class StreamObserverBackPressure<T> implements StreamObserver<T> {

  @Override public Optional<Long> requestBackPressure() {
    return Optional.empty();
  }

  @Override public Optional<Integer> requestBuffer() {
    return Optional.empty();
  }
}
