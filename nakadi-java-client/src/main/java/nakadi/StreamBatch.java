package nakadi;

import java.util.List;

/**
 * Represents the batch of events pushed by the server in the stream.
 *
 * @param <T> the type of the events in the batch
 */
public interface StreamBatch<T> {

  /**
   * The position of the batch in the stream.
   *
   * @return the batch {@link Cursor}
   */
  Cursor cursor();

  /**
   * Notification data sent by the server.
   *
   * @return the batch {@link StreamInfo}
   */
  StreamInfo info();

  /**
   * The events in the batch. This may be an empty list.
   *
   * @return the parameterized events in the batch
   */
  List<T> events();

  /**
   * Convenience method to see if the batch is empty. This happens when the server emits
   * a keepalive batch, which will be empty but contain the most recent cursor.
   *
   * @return true if there no events in this batch
   */
  boolean isEmpty();
}
