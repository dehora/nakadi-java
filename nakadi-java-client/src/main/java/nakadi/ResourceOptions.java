package nakadi;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class ResourceOptions {

  static final String HEADER_ACCEPT = "Accept";
  static final String HEADER_ACCEPT_CHARSET = "Accept-Charset";
  static final String HEADER_CONTENT_TYPE = "Content-Type";

  //multimap would be correct, but a client for this api doesn't setting multiple same headers
  private final Map<String, Object> headers = new HashMap<>();
  private TokenProvider provider;

  public ResourceOptions tokenProvider(TokenProvider provider) {
    NakadiException.throwNonNull(provider, "Please provide a TokenProvider");
    this.provider = provider;
    return this;
  }

  public ResourceOptions header(String name, Object value) {
    NakadiException.throwNonNull(name, "Please provide a header name");
    NakadiException.throwNonNull(value, "Please provide a header value");
    this.headers.put(name, value);
    return this;
  }

  public ResourceOptions headers(Map<String, Object> headers) {
    NakadiException.throwNonNull(headers, "Please provide some headers");
    this.headers.putAll(headers);
    return this;
  }

  /**
   * Convenience method to set the Accept header for the request.
   *
   * @param accept the value of the Content-Type header
   * @return this updated
   */
  public ResourceOptions accept(String accept) {
    this.header(HEADER_ACCEPT, accept);
    return this;
  }

  /**
   * Convenience method to set the Accept Charset header for the request.
   *
   * @param charset the value of the Content-Type header
   * @return this updated
   */
  public ResourceOptions acceptCharset(String charset) {
    this.header(HEADER_ACCEPT_CHARSET, charset);
    return this;
  }

  /**
   * Convenience method to set the Content-Type header for the request.
   *
   * @param contentType the value of the Content-Type header
   * @return this updated
   */
  public ResourceOptions contentType(String contentType) {
    this.header(HEADER_CONTENT_TYPE, contentType);
    return this;
  }


  /**
   * Deprecated since 0.9.7 and will be removed in 0.10.0.
   *
   * <p>
   * Scopes have been removed in recent Nakadi versions. Scopes set here are ignored.
   * </p>
   * @param scope
   * @return this
   */
  @Deprecated
  public ResourceOptions scope(String scope) {
    return this;
  }

  public ResourceOptions flowId(final String flowId) {
    NakadiException.throwNonNull(flowId, "Please provide a flow ID");
    return header("X-Flow-Id", flowId);
  }

  public Map<String, Object> headers() {
    return headers;
  }

  public Optional<String> supplyToken() {
    return provider.authHeaderValue(null);
  }

  /**
   * Deprecated since 0.9.7 and will be removed in 0.10.0.
   *
   * <p>
   * Scopes have been removed in recent Nakadi versions. This always returns null.
   * </p>
   * @return null
   */
  @Deprecated
  public String scope() {
    return null;
  }
}
