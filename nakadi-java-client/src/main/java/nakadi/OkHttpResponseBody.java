package nakadi;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

class OkHttpResponseBody implements ResponseBody {

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
    okResponse.body().close();
  }
}
