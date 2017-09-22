package nakadi;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * An abstraction of the HTTP response from the server.
 */
public interface Response extends Closeable {

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

  /**
   * Closes the response. Applications should call this, or
   * {@link ResponseBody#close()} to ensure underlying connections
   * are released.
   */
  void close();
}
