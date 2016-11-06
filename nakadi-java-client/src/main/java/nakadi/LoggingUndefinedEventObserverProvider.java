package nakadi;

public class LoggingUndefinedEventObserverProvider implements StreamObserverProvider {

  @Override public StreamObserver createStreamObserver() {
    return new LoggingUndefinedEventObserver();
  }

  @Override public TypeLiteral typeLiteral() {
    return new TypeLiteral<UndefinedEventMapped>() {
    };
  }
}
