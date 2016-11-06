package nakadi;

import java.util.Objects;

public class Partition {

  private String partition;
  private String oldestAvailableOffset;
  private String newestAvailableOffset;

  public String partition() {
    return partition;
  }

  public String oldestAvailableOffset() {
    return oldestAvailableOffset;
  }

  public String newestAvailableOffset() {
    return newestAvailableOffset;
  }

  @Override public int hashCode() {
    return Objects.hash(partition, oldestAvailableOffset, newestAvailableOffset);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Partition partition1 = (Partition) o;
    return Objects.equals(partition, partition1.partition) &&
        Objects.equals(oldestAvailableOffset, partition1.oldestAvailableOffset) &&
        Objects.equals(newestAvailableOffset, partition1.newestAvailableOffset);
  }

  @Override public String toString() {
    return "Partition{" + "partition='" + partition + '\'' +
        ", oldestAvailableOffset='" + oldestAvailableOffset + '\'' +
        ", newestAvailableOffset='" + newestAvailableOffset + '\'' +
        '}';
  }
}
