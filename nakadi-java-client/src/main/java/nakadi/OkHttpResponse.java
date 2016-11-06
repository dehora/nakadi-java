package nakadi;

import java.util.List;
import java.util.Map;
import java.util.Objects;

class OkHttpResponse implements Response {

  private final okhttp3.Response okResponse;

  public OkHttpResponse(okhttp3.Response okResponse) {
    this.okResponse = okResponse;
  }

  public int statusCode() {
    return okResponse.code();
  }

  public String reason() {
    return okResponse.message();
  }

  public Map<String, List<String>> headers() {
    return okResponse.headers().toMultimap(); // nb: okhttp already makes a copy here
  }

  @Override public ResponseBody responseBody() {
    return new OkHttpResponseBody(okResponse);
  }

  @Override public int hashCode() {
    return Objects.hash(okResponse);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OkHttpResponse that = (OkHttpResponse) o;
    return Objects.equals(okResponse, that.okResponse);
  }

  @Override public String toString() {
    return "HttpResponse{" + "response=" + okResponse +
        '}';
  }
}
