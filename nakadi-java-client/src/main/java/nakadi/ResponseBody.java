package nakadi;

import java.io.Closeable;
import java.io.InputStream;
import java.io.Reader;

/**
 * Represents the server's HTTP response entity.
 * <p>
 * The entity is consumed once. It's the caller's responsibility to close the underlying
 * resource using {@link #close} when accessing via {@link #asReader} or {@link #asInputStream}.
 * This is suitable for a large or streaming response, eg the kind returned by Nakadi's consumer
 * streams. In that case, prefer to use a {@link #asReader} or {@link #asInputStream}, as a
 * {@link #asString} call is likely to time out unless very carefully configured.</p>
 */
public interface ResponseBody extends Closeable {

  /**
   * The response as a string. This buffers the entire response entity which will then
   * result in a {@link #close}, ie, the client does not have to call {@link #close} after
   * {@link #asString} returns.
   *
   * @return the response body as a String.
   * @throws ClientException if the response cannot be converted to a String.
   */
  String asString() throws ClientException;

  /**
   * The response as a Reader.
   *
   * @return the response body as a Reader.
   */
  Reader asReader();

  /**
   * The response as an InputStream. Suitable for binary responses.
   *
   * @return the response body as an InputStream.
   */
  InputStream asInputStream();

  /**
   * The Content-Type header value as a String.
   *
   * @return the media type String
   */
  String mediaTypeString();

  /**
   * The content length in bytes. A value of 0 indicates an empty body. A value of -1
   * indicates the length is unknown.
   *
   * @return the content length
   */
  long contentLength();
}
