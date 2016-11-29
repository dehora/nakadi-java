package nakadi;

/**
 * Helper class to provide {@link LoggingStreamObserver}.
 */
public class LoggingStreamObserverProvider implements StreamObserverProvider<String> {

  @Override public StreamObserver<String> createStreamObserver() {
    return new LoggingStreamObserver();
  }

  @Override public TypeLiteral<String> typeLiteral() {
    return TypeLiterals.OF_STRING;
  }
}
