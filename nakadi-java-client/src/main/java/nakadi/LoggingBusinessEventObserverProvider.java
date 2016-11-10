package nakadi;

/**
 * Helper class to provide {@link LoggingBusinessEventObserver}
 */
public class LoggingBusinessEventObserverProvider implements StreamObserverProvider {

  @Override public StreamObserver createStreamObserver() {
    return new LoggingBusinessEventObserver();
  }

  @Override public TypeLiteral typeLiteral() {
    return new TypeLiteral<BusinessEventMapped>() {
    };
  }
}
