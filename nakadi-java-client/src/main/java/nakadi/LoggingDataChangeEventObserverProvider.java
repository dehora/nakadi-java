package nakadi;

import java.util.Map;

/**
 * Helper class to provide {@link DataChangeEvent} with Map data.
 */
public class LoggingDataChangeEventObserverProvider
    implements StreamObserverProvider<DataChangeEvent<Map<String, Object>>> {

  @Override public StreamObserver<DataChangeEvent<Map<String, Object>>> createStreamObserver() {
    return new LoggingDataChangeEventObserver();
  }

  @Override public TypeLiteral<DataChangeEvent<Map<String, Object>>> typeLiteral() {
    return TypeLiterals.OF_DATA_MAP;
  }
}
