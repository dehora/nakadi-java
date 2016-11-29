package nakadi;

import java.util.Map;

/**
 * Helper class to provide {@link LoggingUndefinedEventObserver}.
 */
public class LoggingUndefinedEventObserverProvider
    implements StreamObserverProvider<UndefinedEventMapped<Map<String, Object>>> {

  @Override public StreamObserver<UndefinedEventMapped<Map<String, Object>>> createStreamObserver() {
    return new LoggingUndefinedEventObserver();
  }

  @Override public TypeLiteral<UndefinedEventMapped<Map<String, Object>>> typeLiteral() {
    return TypeLiterals.OF_UNDEFINED_MAP;
  }
}
