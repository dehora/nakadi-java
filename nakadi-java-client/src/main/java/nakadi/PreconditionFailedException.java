package nakadi;

public class PreconditionFailedException extends NakadiException {

  public PreconditionFailedException(Problem problem) {
    super(problem);
  }

  public PreconditionFailedException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
