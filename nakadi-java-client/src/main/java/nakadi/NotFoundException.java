package nakadi;

public class NotFoundException extends NakadiException {

  public NotFoundException(Problem problem) {
    super(problem);
  }

  public NotFoundException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
