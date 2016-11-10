package nakadi;

/**
 * Helper class to provide {@link LoggingStreamObserver}.
 */
public class LoggingStreamObserverProvider implements StreamObserverProvider {

  @Override public StreamObserver createStreamObserver() {
    return new LoggingStreamObserver();
  }

  @Override public TypeLiteral typeLiteral() {
    return new TypeLiteral<String>() {
    };
  }
}
