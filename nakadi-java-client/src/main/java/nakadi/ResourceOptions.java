package nakadi;

import com.google.common.collect.Maps;
import java.util.Map;

class ResourceOptions {

  /*
   todo: multimap would be correct, but a client setting multiple same headers
   is less than an edge case
    */
  private final Map<String, Object> headers = Maps.newHashMap();
  private TokenProvider provider;

  public ResourceOptions tokenProvider(TokenProvider provider) {
    this.provider = provider;
    return this;
  }

  public ResourceOptions header(String name, Object value) {
    this.headers.put(name, value);
    return this;
  }

  public ResourceOptions headers(Map<String, Object> headers) {
    this.headers.putAll(headers);
    return this;
  }

  public Map<String, Object> headers() {
    return headers;
  }

  public String supplyToken() {
    return provider.authHeaderValue();
  }
}
