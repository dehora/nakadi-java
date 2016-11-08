package nakadi;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OkHttpResponseBody implements ResponseBody {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());
  private static final int CLOSE_ATTEMPTS = 3;

  private final okhttp3.Response okResponse;

  OkHttpResponseBody(okhttp3.Response okResponse) {
    this.okResponse = okResponse;
  }

  @Override
  public String asString() throws ClientException {
    try {
      return okResponse.body().string();
    } catch (IOException e) {
      throw new ClientException(Problem.localProblem("could not string body", e.getMessage()), e);
    }
  }

  @Override
  public Reader asReader() {
    return okResponse.body().charStream();
  }

  @Override
  public InputStream asInputStream() {
    return okResponse.body().byteStream();
  }

  @Override
  public String mediaTypeString() {
    return okResponse.body().contentType().toString();
  }

  @Override
  public long contentLength() {
    return okResponse.body().contentLength();
  }

  @Override public void close() throws IOException {
    boolean closed = false;
    try {
      okResponse.body().close();
      closed = true;
    } catch (Exception e) {
      logger.warn("okhttp_close_error problem closing on {} {}", e.getClass().getName(), e.getMessage());
    } finally {
      // try again, but it looks like you get one shot with okhttp, esp. for a cross thread problem
      int attempt = 0;
      while(!closed && attempt++ < CLOSE_ATTEMPTS) {
        try {
          okResponse.close();
          closed = true;
        } catch (Exception e1) {
          logger.warn("okhttp_close_error retrying close attempts {}/{} on {}", attempt,
              CLOSE_ATTEMPTS, e1.getMessage());
        }
      }

      if(!closed) {
        logger.error("okhttp_close_error could not close http response attempts {}/{}", attempt,
            CLOSE_ATTEMPTS);
      }
    }
  }
}
