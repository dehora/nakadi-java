package nakadi;

/**
 * Helper class to provide {@link LoggingUndefinedEventObserver}.
 */
public class LoggingUndefinedEventObserverProvider implements StreamObserverProvider {

  @Override public StreamObserver createStreamObserver() {
    return new LoggingUndefinedEventObserver();
  }

  @Override public TypeLiteral typeLiteral() {
    return new TypeLiteral<UndefinedEventMapped<String>>() {
    };
  }
}
