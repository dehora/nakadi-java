package nakadi;

import java.util.Optional;

abstract public class StreamObserverBackPressure<T> implements StreamObserver<T> {

  @Override public Optional<Long> requestBackPressure() {
    return Optional.empty();
  }

  @Override public Optional<Integer> requestBuffer() {
    return Optional.empty();
  }
}
