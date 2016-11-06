package nakadi;

public class InvalidException extends NakadiException {

  public InvalidException(Problem problem) {
    super(problem);
  }

  public InvalidException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
