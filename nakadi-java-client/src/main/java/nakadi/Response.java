package nakadi;

import java.util.List;
import java.util.Map;

/**
 * An abstraction of the HTTP response from the server.
 */
public interface Response {

  /**
   * The HTTP status code
   *
   * @return a status code
   */
  int statusCode();

  /**
   * The reason associated with the code, eg "Ok" for a 200.
   *
   * @return the HTTP reason
   */
  String reason();

  /**
   * The response HTTP headers.
   *
   * @return the response headers.
   */
  Map<String, List<String>> headers();

  /**
   * The server response body. This can be null.
   *
   * @return the {@link ResponseBody}
   */
  ResponseBody responseBody();
}
