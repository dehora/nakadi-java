package nakadi;

/**
 * An exception representing a server (5xx) response code.
 */
public class ServerException extends NakadiException {

  /**
   * @param problem the Problem detail
   */
  public ServerException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public ServerException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
