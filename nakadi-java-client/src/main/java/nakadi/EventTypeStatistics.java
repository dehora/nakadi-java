package nakadi;

import java.util.Objects;

/**
 * The API event type statistics.
 */
public class EventTypeStatistics {

  private int messagesPerMinute;
  private int messageSize;
  private int readParallelism;
  private int writeParallelism;

  /**
   * @return the messages per minute
   */
  public int messagesPerMinute() {
    return messagesPerMinute;
  }

  /**
   * @return the message size
   */
  public int messageSize() {
    return messageSize;
  }

  /**
   * @return the read parallelism
   */
  public int readParallelism() {
    return readParallelism;
  }

  /**
   * @return the write parallelism
   */
  public int writeParallelism() {
    return writeParallelism;
  }

  @Override public int hashCode() {
    return Objects.hash(messagesPerMinute, messageSize, readParallelism, writeParallelism);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventTypeStatistics that = (EventTypeStatistics) o;
    return messagesPerMinute == that.messagesPerMinute &&
        messageSize == that.messageSize &&
        readParallelism == that.readParallelism &&
        writeParallelism == that.writeParallelism;
  }

  @Override public String toString() {
    return "EventTypeStatistics{" + "messagesPerMinute=" + messagesPerMinute +
        ", messageSize=" + messageSize +
        ", readParallelism=" + readParallelism +
        ", writeParallelism=" + writeParallelism +
        '}';
  }
}
