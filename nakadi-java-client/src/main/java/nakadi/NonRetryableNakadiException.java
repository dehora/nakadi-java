package nakadi;

public class NonRetryableNakadiException extends NakadiException {

  public NonRetryableNakadiException(Problem problem) {
    super(problem);
  }

  public NonRetryableNakadiException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
