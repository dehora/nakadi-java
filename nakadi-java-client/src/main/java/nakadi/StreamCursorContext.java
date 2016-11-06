package nakadi;

import java.util.Map;

/**
 * Captures the {@link Cursor} and extra context from a subscription stream batch.
 */
public interface StreamCursorContext {

  /**
   * The {@link Cursor} object returned in the batch.
   *
   * @return The cursor object returned in the batch
   */
  Cursor cursor();

  /**
   * Extension point for sending along extra information to a {@link StreamOffsetObserver}. For the
   * subscription stream this will include the key {@link StreamResourceSupport#X_NAKADI_STREAM_ID}
   * and the key {@link StreamResourceSupport#SUBSCRIPTION_ID}
   *
   * @return a map with extra information.
   */
  Map<String, String> context();
}
