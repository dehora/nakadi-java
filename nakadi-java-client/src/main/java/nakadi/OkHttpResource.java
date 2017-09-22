package nakadi;

import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.schedulers.Schedulers;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static nakadi.MetricCollector.Meter.retrySkipFinished;

class OkHttpResource implements Resource {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private static final String HEADER_AUTHORIZATION = "Authorization";
  private static final String APPLICATION_JSON_CHARSET_UTF8 = "application/json; charset=utf8";

  private final OkHttpClient okHttpClient;
  private final JsonSupport jsonSupport;
  private final MetricCollector metricCollector;
  private long connectTimeout = 0;
  private long readTimeout = 0;
  private volatile boolean hasPerRequestConnectTimeout;
  private volatile boolean hasPerRequestReadTimeout;
  private volatile RetryPolicy retryPolicy;
  private volatile Response response;

  OkHttpResource(OkHttpClient okHttpClient, JsonSupport jsonSupport, MetricCollector collector) {
    NakadiException.throwNonNull(okHttpClient, "Please provide a client");
    NakadiException.throwNonNull(jsonSupport, "Please provide JSON support");
    NakadiException.throwNonNull(collector, "Please provide a metric collector");
    this.okHttpClient = okHttpClient;
    this.jsonSupport = jsonSupport;
    this.metricCollector = collector;
  }

  public OkHttpResource connectTimeout(long timeout, TimeUnit unit) {
    NakadiException.throwNonNull(unit, "Please provide a time unit");
    this.connectTimeout = unit.toMillis(timeout);
    hasPerRequestConnectTimeout = true;
    return this;
  }

  public OkHttpResource readTimeout(long timeout, TimeUnit unit) {
    NakadiException.throwNonNull(unit, "Please provide a time unit");
    this.readTimeout = unit.toMillis(timeout);
    hasPerRequestReadTimeout = true;
    return this;
  }

  public OkHttpResource retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  @Override
  public Response request(String method, String url, ResourceOptions options)
      throws NakadiException {

    // If we have a retry in place use a throwing call and return the last response captured.
    // the rx retry mechanism is driven by exceptions but the caller is asking for a response here
    // so we don't propagate the exception
    if (retryPolicy != null) {
      if (retryPolicy.isFinished()) {
        logger.warn("no_retry_cowardly refusing to apply finished retry policy {}", retryPolicy);
        metricCollector.mark(retrySkipFinished);
      } else {
        Response first = null;
        try {
          Observable<Response> observable =
              Observable.defer(
                  () -> Observable.just(requestThrowingInner(method, url, options, null)));
          first = observable.compose(buildRetry(retryPolicy)).blockingFirst();
          releaseResponseQuietly();
          return first;
        } catch (HttpException e) {
          if(first != null) {
            ResponseSupport.closeQuietly(first);
          }

          logger.error("retryable_request_failed, {}", e.getMessage(), e);
          return response;
        }
      }
    }

    return Observable.defer(
        () -> Observable.just(requestInner(method, url, options, null))).blockingFirst();
  }

  @Override public Response requestThrowing(String method, String url, ResourceOptions options)
      throws NakadiException {

    return maybeComposeRetryPolicy(
        Observable.defer(
            () -> Observable.just(requestThrowingInner(method, url, options)))).blockingFirst();
  }

  @Override
  public Response requestThrowing(String method, String url, ResourceOptions options,
      ContentSupplier body)
      throws NakadiException {

    return maybeComposeRetryPolicy(
        Observable.defer(() -> Observable.just(
            requestThrowingInner(method, url, options, body)))).blockingFirst();
  }

  @Override
  public Response postEventsThrowing(String url, ResourceOptions options, ContentSupplier body)
      throws NakadiException {

    return maybeComposeRetryPolicy(
        Observable.defer(() -> Observable.just(
            throwPostEventsIfError(requestInner(POST, url, options, body))))).blockingFirst();
  }

  @Override
  public <Res> Res requestThrowing(String method, String url, ResourceOptions options,
      Class<Res> res) throws NakadiException {

    Response response = maybeComposeRetryPolicy(
        Observable.defer(() -> Observable.just(
            requestThrowingInner(method, url, options)))).blockingFirst();
    return marshalResponse(response, res);
  }

  @Override
  public <Res> Res requestThrowing(String method, String url, ResourceOptions options,
      ContentSupplier body, Class<Res> res)
      throws NakadiException {

    Response response = maybeComposeRetryPolicy(
        Observable.defer(() -> Observable.just(
            requestThrowingInner(method, url, options, body)))).blockingFirst();
    return marshalResponse(response, res);
  }

  private void releaseResponseQuietly() {
    if (response != null) {
      try {
        ResponseSupport.closeQuietly(response, 1);
      } finally {
        response = null; // zero out any previous responses
      }
    }
  }

  private <Req> Response requestThrowingInner(String method, String url, ResourceOptions options,
      ContentSupplier body) {
    return throwIfError(requestInner(method, url, options, body));
  }

  private <Req> Response requestInner(String method, String url, ResourceOptions options,
      ContentSupplier body) {
    return okHttpRequest(prepareBuilder(method, url, options, body));
  }

  private Response requestThrowingInner(String method, String url, ResourceOptions options) {
    return requestThrowingInner(method, url, options, null);
  }

  private Observable<Response> maybeComposeRetryPolicy(final Observable<Response> observable) {
    if (retryPolicy != null) {
      if (retryPolicy.isFinished()) {
        // this can happen if
        // a) the policy is a no-op policy always returning finished
        // b) it's being reused across requests, likely a client bug
        logger.warn("no_retry_cowardly refusing to used finished retry policy {}", retryPolicy);
        metricCollector.mark(retrySkipFinished);
      } else {
        return observable.compose(buildRetry(retryPolicy));
      }
    }
    return observable;
  }

  private Request.Builder prepareBuilder(String method, String url, ResourceOptions options,
      ContentSupplier body) {
    Request.Builder builder;
    if (body != null) {
      {
        RequestBody requestBody =
            RequestBody.create(MediaType.parse(APPLICATION_JSON_CHARSET_UTF8), body.content());
        builder = new Request.Builder().url(url).method(method, requestBody);
      }
    } else {
      builder = applyMethodForNoBody(method, url, new Request.Builder().url(url));
    }
    options.headers()
        .entrySet()
        .stream()
         // okhttp automatically sets up and decompresses Accept-Encoding: gzip
         // setting it manually requires manual decompression, so a supplied option is best skipped
        .filter(this::filterAcceptEncodingGzip)
        .forEach(e -> builder.addHeader(e.getKey(), e.getValue().toString()));

    applyAuthHeaderIfPresent(options, builder);
    return builder;
  }

  private boolean filterAcceptEncodingGzip(Map.Entry<String, Object> e) {
    return !"Accept-Encoding".equalsIgnoreCase(e.getKey()) || !"gzip".equalsIgnoreCase(
        e.getValue().toString());
  }

  @SuppressWarnings("WeakerAccess") @VisibleForTesting
  Response okHttpRequest(Request.Builder builder) {
    try {
      return new OkHttpResponse(okHttpCall(builder));
    } catch (IOException e) {
      throw new RetryableException(Problem.networkProblem(e.getMessage(), ""), e);
    }
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

  private void applyAuthHeaderIfPresent(ResourceOptions options, Request.Builder builder) {
    options.supplyToken().ifPresent(t -> builder.header(HEADER_AUTHORIZATION, t));
  }

  private ObservableTransformer<Response, Response> buildRetry(RetryPolicy backoff) {
    return new RequestRetry()
        .retryWhenWithBackoffObserver(
            backoff,
            Schedulers.computation(),
            ExceptionSupport::isApiRequestRetryable);
  }

  private Request.Builder applyMethodForNoBody(String method, String url, Request.Builder builder) {
    // assume we're not dealing with put/post/patch here as there's no body
    if (Resource.DELETE.equals(method)) {
      return builder.delete();
    } else if (Resource.GET.equals(method)) {
      return builder.get();
    } else if (Resource.HEAD.equals(method)) {
      return builder.head();
    } else {
      logger.warn("unexpected_method_request_with_no_body method={} url={}", method, url);
      return builder.method(method, RequestBody.create(MediaType.parse("text/plain"), ""));
    }
  }

  private <Res> Res marshalResponse(Response response, Class<Res> res) {
    if (res != null && res.isAssignableFrom(Response.class)) {
      //noinspection unchecked  // safe cast
      return (Res) response;
    } else {
      // asString will drive a underlying close of the http connection
      return jsonSupport.fromJson(response.responseBody().asString(), res);
    }
  }

  private Response throwIfError(Response response) {
    int code = response.statusCode();
    if (code >= 200 && code < 300) {
      return response;
    } else {
       // field the response as this allows us to return it when a non-throwing request
       // with a retry fails (we have to wrap non-throwing requests as throwing to
       // drive the rx retry mechanism)
      this.response = response;
      return handleError(response);
    }
  }

  private Response throwPostEventsIfError(Response response) {
    int code = response.statusCode();
    if (code == 207 || code == 422) {
      return response;
    }
    return throwIfError(response);
  }

  private <T> T handleError(Response response) throws ContractRetryableException {
    final Problem problem = ProblemSupport.toProblem(response, jsonSupport);
    // use problem status instead of http code, also a workaround for #nakadi/issues/645
    return throwProblem(problem.status(), problem);
  }

  private <T> T throwProblem(int code, Problem problem) {
    return ProblemSupport.throwProblem(code, problem, metricCollector);
  }
}
