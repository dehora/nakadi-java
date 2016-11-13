package nakadi;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;

class ResourceOptions {

  /*
   todo: multimap would be correct, but a client setting multiple same headers
   is less than an edge case
    */
  private final Map<String, Object> headers = Maps.newHashMap();
  private TokenProvider provider;
  private String scope = TokenProvider.UID;

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

  public ResourceOptions scope(String scope) {
    NakadiException.throwNonNull(scope, "Please provide a scope");
    this.scope = scope;
    return this;
  }

  public Map<String, Object> headers() {
    return headers;
  }

  public Optional<String> supplyToken() {
    return provider.authHeaderValue(this.scope);
  }
}
