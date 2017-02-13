package nakadi;

import java.util.Objects;

public class BatchItemResponse {

  private String eid;
  private PublishingStatus publishing_status;
  private Step step;
  private String detail;

  public String eid() {
    return eid;
  }

  public PublishingStatus publishingStatus() {
    return publishing_status;
  }

  public Step step() {
    return step;
  }

  public String detail() {
    return detail;
  }

  @Override public int hashCode() {
    return Objects.hash(eid, publishing_status, step, detail);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BatchItemResponse that = (BatchItemResponse) o;
    return Objects.equals(eid, that.eid) &&
        publishing_status == that.publishing_status &&
        step == that.step &&
        Objects.equals(detail, that.detail);
  }

  @Override public String toString() {
    return "BatchItemResponse{" + "eid='" + eid + '\'' +
        ", publishing_status=" + publishing_status +
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
