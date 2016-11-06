package nakadi;

import java.util.Objects;

public class EventTypeStatistics {

  private int messagesPerMinute;
  private int messageSize;
  private int readParallelism;
  private int writeParallelism;

  public int messagesPerMinute() {
    return messagesPerMinute;
  }

  public int messageSize() {
    return messageSize;
  }

  public int readParallelism() {
    return readParallelism;
  }

  public EventTypeStatistics readParallelism(int readParallelism) {
    this.readParallelism = readParallelism;
    return this;
  }

  public int writeParallelism() {
    return writeParallelism;
  }

  public EventTypeStatistics writeParallelism(int writeParallelism) {
    this.writeParallelism = writeParallelism;
    return this;
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
