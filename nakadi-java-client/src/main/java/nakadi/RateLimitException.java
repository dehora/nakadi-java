package nakadi;

public class RateLimitException extends NakadiException {

  public RateLimitException(Problem problem) {
    super(problem);
  }

  public RateLimitException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
