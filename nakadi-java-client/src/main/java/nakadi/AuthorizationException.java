package nakadi;

public class AuthorizationException extends NakadiException {

  public AuthorizationException(Problem problem) {
    super(problem);
  }

  public AuthorizationException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
