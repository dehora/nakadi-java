package nakadi;

public class ConflictException extends NakadiException {

  public ConflictException(Problem problem) {
    super(problem);
  }

  public ConflictException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
