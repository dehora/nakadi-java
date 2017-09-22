package nakadi;

import java.io.Closeable;
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
      throw new ContractRetryableException(
          Problem.contractRetryableProblem("missing_response_body", e.getMessage()), e);
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
    ResponseSupport.closeQuietly(this.okResponse, CLOSE_ATTEMPTS);
  }

}
