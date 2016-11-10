package nakadi;

/**
 * An exception representing a network level problem, such as inability to access the network, or
 * make an outbound connection.
 */
public class NetworkException extends NakadiException {

  /**
   * @param problem the Problem detail
   */
  public NetworkException(Problem problem) {
    super(problem);
  }

  /**
   * @param problem the Problem detail
   * @param cause the cause
   */
  public NetworkException(Problem problem, Throwable cause) {
    super(problem, cause);
  }
}
