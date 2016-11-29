package nakadi;

import java.util.Map;

/**
 * Helper class to provide {@link LoggingBusinessEventObserver} with Map data.
 */
public class LoggingBusinessEventObserverProvider
    implements StreamObserverProvider<BusinessEventMapped<Map<String, Object>>> {

  @Override public StreamObserver<BusinessEventMapped<Map<String, Object>>> createStreamObserver() {
    return new LoggingBusinessEventObserver();
  }

  @Override public TypeLiteral<BusinessEventMapped<Map<String, Object>>> typeLiteral() {
    return TypeLiterals.OF_BUSINESS_MAP;
  }
}
