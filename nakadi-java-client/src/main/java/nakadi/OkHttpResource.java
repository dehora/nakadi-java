package nakadi;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import rx.Observable;
import rx.schedulers.Schedulers;

class OkHttpResource implements Resource {

  private static final String HEADER_AUTHORIZATION = "Authorization";
  private static final String APPLICATION_JSON_CHARSET_UTF8 = "application/json; charset=utf8";

  private final OkHttpClient okHttpClient;
  private final JsonSupport jsonSupport;
  private final MetricCollector metricCollector;
  private long connectTimeout = 0;
  private long readTimeout = 0;
  private volatile boolean hasPerRequestConnectTimeout;
  private volatile boolean hasPerRequestReadTimeout;

  OkHttpResource(OkHttpClient okHttpClient, JsonSupport jsonSupport, MetricCollector collector) {
    this.okHttpClient = okHttpClient;
    this.jsonSupport = jsonSupport;
    this.metricCollector = collector;
  }

  public OkHttpResource connectTimeout(long timeout, TimeUnit unit) {
    this.connectTimeout = unit.toMillis(timeout);
    hasPerRequestConnectTimeout = true;
    return this;
  }

  public OkHttpResource readTimeout(long timeout, TimeUnit unit) {
    this.readTimeout = unit.toMillis(timeout);
    hasPerRequestReadTimeout = true;
    return this;
  }

  @Override
  public Response request(String method, String url, ResourceOptions options)
      throws NakadiException {
    try {
      Request.Builder builder = applyMethodForNoBody(method, new Request.Builder().url(url));
      options.headers().entrySet()
          .forEach(e -> builder.addHeader(e.getKey(), e.getValue().toString()));

      applyAuthHeaderIfPresent(options, builder);

      return new OkHttpResponse(okHttpCall(builder));
    } catch (IOException e) {
      throw new NetworkException(Problem.networkProblem(e.getMessage(), ""), e);
    }
  }

  private void applyAuthHeaderIfPresent(ResourceOptions options, Request.Builder builder) {
    String token = options.supplyToken();
    if(token != null && !"".equals(token.trim())) {
      builder.header(HEADER_AUTHORIZATION, token);
    }
  }

  @Override
  public <Req> Response request(String method, String url, ResourceOptions options, Req body)
      throws NakadiException {
    try {
      RequestBody requestBody =
          RequestBody.create(MediaType.parse(APPLICATION_JSON_CHARSET_UTF8),
              jsonSupport.toJson(body));
      Request.Builder builder = new Request.Builder().url(url).method(method, requestBody);
      options.headers().entrySet()
          .forEach(e -> builder.addHeader(e.getKey(), e.getValue().toString()));
      applyAuthHeaderIfPresent(options, builder);
      return new OkHttpResponse(okHttpCall(builder));
    } catch (IOException e) {
      throw new NetworkException(Problem.networkProblem(e.getMessage(), ""), e);
    }
  }

  @Override public Response requestThrowing(String method, String url, ResourceOptions options)
      throws NakadiException {
    return throwIfError(request(method, url, options));
  }

  @Override
  public <Req> Response requestThrowing(String method, String url, ResourceOptions options,
      Req body) throws NakadiException {
    return throwIfError(request(method, url, options, body));
  }

  @Override
  public <Req> Response requestRetryThrowing(String method, String url, ResourceOptions options, Req body , PolicyBackoff backoff)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    // defer gives us a chance to register a retry (requestThrowing results in a hot observable)
    Observable<Response> observable = Observable.defer(
        () -> Observable.just(requestThrowing(method, url, options, body))
    ).compose(buildRetry(backoff));

    return observable.toBlocking().first();
  }

  @Override
  public <Res> Res requestRetryThrowing(
      String method, String url, ResourceOptions options, Class<Res> res, PolicyBackoff backoff) {
    // defer gives us a chance to register a retry (requestThrowing results in a hot observable)
    Observable<Response> observable = Observable.defer(() ->
        Observable.just(throwIfError(request(method, url, options)))
    ).compose(buildRetry(backoff));

    return marshalResponse(observable.toBlocking().first(), res);
  }

  @Override
  public <Res> Res requestThrowing(String method, String url, ResourceOptions options,
      Class<Res> res) throws NakadiException {
    return marshalResponse(throwIfError(request(method, url, options)), res);
  }


  @Override
  public <Res, Req> Res requestThrowing(String method, String url, ResourceOptions options,
      Req body,
      Class<Res> res) {
    Response request = request(method, url, options, body);
    return marshalResponse(throwIfError(request), res);
  }

  private Observable.Transformer<Response, Response> buildRetry(PolicyBackoff backoff) {
    return new StreamConnectionRetry()
        .retryWhenWithBackoff(
            backoff, Schedulers.computation(), StreamExceptionSupport::isRetryable);
  }

  private okhttp3.Response okHttpCall(Request.Builder builder) throws IOException {

    if (hasPerRequestReadTimeout || hasPerRequestConnectTimeout) {

      final OkHttpClient.Builder clientBuilder = okHttpClient.newBuilder();
      if (hasPerRequestReadTimeout) {
        clientBuilder.readTimeout(readTimeout, TimeUnit.MILLISECONDS);
      }

      if (hasPerRequestConnectTimeout) {
        clientBuilder.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS);
      }
      return clientBuilder.build().newCall(builder.build()).execute();
    } else {
      return okHttpClient.newCall(builder.build()).execute();
    }
  }

  private Request.Builder applyMethodForNoBody(String method, Request.Builder builder) {
    // assume we're not dealing with put/post/patch here as there's no body
    if (Resource.DELETE.equals(method)) {
      builder = builder.delete();
    } else if (Resource.GET.equals(method)) {
      builder = builder.get();
    } else if (Resource.HEAD.equals(method)) {
      builder = builder.head();
    } else {
      // todo: hack, fix this
      builder = builder.method(method, RequestBody.create(MediaType.parse("text/plain"), ""));
    }
    return builder;
  }

  private <Res> Res marshalResponse(Response response, Class<Res> res) {
    if (res.isAssignableFrom(Response.class)) {
      //noinspection unchecked  // safe cast
      return (Res) response;
    } else {
      return jsonSupport.fromJson(response.responseBody().asString(), res);
    }
  }

  private Response throwIfError(Response response) {
    int code = response.statusCode();
    if (code >= 200 && code < 300) {
      return response;
    } else {
      return handleError(response);
    }
  }

  private <T> T handleError(Response response) {
    return throwProblem(response.statusCode(),
        jsonSupport.fromJson(response.responseBody().asString(), Problem.class));
  }

  private <T> T throwProblem(int code, Problem problem) {
    if (code == 403 || code == 401) {
      metricCollector.mark(MetricCollector.Meter.http401);
      throw new AuthorizationException(problem);
    } else if (code == 404 || code == 410) {
      metricCollector.mark(MetricCollector.Meter.http404);
      throw new NotFoundException(problem);
    } else if (code == 409) {
      metricCollector.mark(MetricCollector.Meter.http409);
      throw new ConflictException(problem);
    } else if (code == 412) {
      metricCollector.mark(MetricCollector.Meter.http412);
      // eg bad cursors: Precondition Failed; offset 98 for partition 0 is unavailable (412)
      throw new PreconditionFailedException(problem);
    } else if (code == 422) {
      metricCollector.mark(MetricCollector.Meter.http422);
      throw new InvalidException(problem);
    } else if (code == 429) {
      metricCollector.mark(MetricCollector.Meter.http429);
      throw new RateLimitException(problem);
    } else if (code == 400) {
      metricCollector.mark(MetricCollector.Meter.http400);
      throw new ClientException(problem);
    } else if (code >= 400 && code < 500) {
      metricCollector.mark(MetricCollector.Meter.http4xx);
      throw new ClientException(problem);
    } else if (code == 500) {
      metricCollector.mark(MetricCollector.Meter.http500);
      throw new ServerException(problem);
    } else if (code == 503) {
      metricCollector.mark(MetricCollector.Meter.http503);
      throw new ServerException(problem);
    } else if (code > 500 && code < 600) {
      metricCollector.mark(MetricCollector.Meter.http5xx);
      throw new ServerException(problem);
    } else {
      metricCollector.mark(MetricCollector.Meter.httpUnknown);
      throw new NakadiException(problem);
    }
  }
}
