package nakadi;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

class UriBuilder {

  private static Charset UTF_8 = StandardCharsets.UTF_8;
  private static final String AND = "&";
  private static final String EQ = "=";
  private final QueryParams qp = new QueryParams();
  private StringBuilder stringBuilder;

  private UriBuilder(String uri) {

    try {
      URI tmp = new URI(uri);
      if (tmp.getQuery() != null) {
        /*
          replacing params is supported, so catch the params part here and put it into the
          query params object. This leaves a base uri behind which is put into the string builder.
        */
        String query = tmp.getQuery();
        final String[] split = query.split(AND);
        for (String s : split) {
          final String[] split1 = s.split(EQ);
          qp.param(split1[0], split1[1]);
        }
        tmp = new URI(tmp.getScheme(),
            tmp.getUserInfo(),
            tmp.getHost(),
            tmp.getPort(),
            tmp.getPath(),
            null,
            tmp.getFragment());
      }
      this.stringBuilder = new StringBuilder(tmp.toASCIIString());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private UriBuilder(URI uri) {
    this.stringBuilder = new StringBuilder(uri.toASCIIString());
  }

  public static UriBuilder builder(String uri) {
    return new UriBuilder(uri);
  }

  public static UriBuilder builder(URI uri) {
    return new UriBuilder(uri);
  }

  public UriBuilder path(String path) {
    stringBuilder.append("/").append(urlEncode(path));
    return this;
  }

  public UriBuilder query(String param, String value) {
    qp.param(urlEncode(param), urlEncode(value));
    return this;
  }

  public UriBuilder queryReplacing(String param, String value) {
    qp.paramReplacing(urlEncode(param), urlEncode(value));
    return this;
  }

  public UriBuilder query(String param, String... values) {
    final String[] encodedValues = Arrays.stream(values)
        .map(this::urlEncode).collect(Collectors.toList())
        .stream()
        .toArray(String[]::new);
    qp.param(urlEncode(param), encodedValues);
    return this;
  }

  public UriBuilder query(QueryParams params) {
    params.params().entrySet().forEach(e -> {

      final String[] encoded =
          e.getValue().stream()
              .map(this::urlEncode)
              .collect(Collectors.toList())
              .stream()
              .toArray(String[]::new);

      if (params.paramsToReplace().contains(e.getKey())) {
        // propagate replacements to underlying stringBuilder; useful for things like limits/offset
        this.qp.paramReplacing(e.getKey(), encoded);
      } else {
        this.qp.param(e.getKey(), encoded);
      }
    });
    return this;
  }

  String buildString() {
    return build().toASCIIString();
  }

  public URI build() {
    final Map<String, Collection<String>> params = qp.params();
    if (!params.isEmpty()) {
      stringBuilder.append("?");
      StringBuilder sb = new StringBuilder();
      params.entrySet().forEach(q -> {
        String key = q.getKey();
        q.getValue().forEach(e -> sb.append(key).append("=").append(e).append("&"));
      });
      if (sb.lastIndexOf("&") == sb.length() - 1) {
        sb.deleteCharAt(sb.length() - 1);
      }
      stringBuilder.append(sb);
    }

    try {
      return new URI(stringBuilder.toString());
    } catch (URISyntaxException e) {
      throw new ClientException(Problem.localProblem("could not create api url", null), e);
    }
  }

  private String urlEncode(String param) {
    try {
      // URLEncoder is a html forms encoder not a percent encoder
      return java.net.URLEncoder.encode(param, UTF_8.name()).replaceAll("\\+", "%20");
    } catch (UnsupportedEncodingException e) {
      throw new ClientException(
          Problem.localProblem("Could not encode param", "param=[" + param + "]"), e);
    }
  }
}
