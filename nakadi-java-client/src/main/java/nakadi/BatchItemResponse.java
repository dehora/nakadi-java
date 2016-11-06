package nakadi;

import java.util.Objects;

public class BatchItemResponse {

  private String eid;
  private PublishingStatus status;
  private Step step;
  private String detail;

  public String eid() {
    return eid;
  }

  public PublishingStatus status() {
    return status;
  }

  public Step step() {
    return step;
  }

  public String detail() {
    return detail;
  }

  @Override public int hashCode() {
    return Objects.hash(eid, status, step, detail);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BatchItemResponse that = (BatchItemResponse) o;
    return Objects.equals(eid, that.eid) &&
        status == that.status &&
        step == that.step &&
        Objects.equals(detail, that.detail);
  }

  @Override public String toString() {
    return "BatchItemResponse{" + "eid='" + eid + '\'' +
        ", status=" + status +
        ", step=" + step +
        ", detail='" + detail + '\'' +
        '}';
  }

  enum PublishingStatus {
    failed, submitted, aborted
  }

  enum Step {
    none, validating, partitioning, enriching, publishing
  }
}
