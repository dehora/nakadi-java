package nakadi;

public class LoggingStreamObserverProvider implements StreamObserverProvider {

  @Override public StreamObserver createStreamObserver() {
    return new LoggingStreamObserver();
  }

  @Override public TypeLiteral typeLiteral() {
    return new TypeLiteral<String>() {
    };
  }
}
