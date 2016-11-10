package nakadi;

import java.util.concurrent.TimeUnit;

/**
 * Supports API operations related to health checks.
 */
public class HealthCheckResource {

  private final NakadiClient client;

  public HealthCheckResource(NakadiClient client) {
    this.client = client;
  }

  /**
   * Make a healthcheck requestThrowing to the server. Non-success codes do not result in
   * exceptions - callers should examine the response.
   *
   * @return a http Response
   * @throws NakadiException typically a local or network exception
   */
  public Response healthcheck() throws NakadiException {

    Resource resource = client.resourceProvider().newResource();
    return resource.request("GET",
        UriBuilder.builder(client.baseURI()).path("health").buildString(),
        ResourceSupport.options("*/*").tokenProvider(client.resourceTokenProvider()));
  }

  /**
   * Make a healthcheck requestThrowing to the server. Non-success codes result in
   * exceptions.
   *
   * @return a http Response if successful
   * @throws AuthorizationException
   * @throws ClientException
   * @throws ServerException
   * @throws InvalidException
   * @throws RateLimitException
   * @throws NakadiException
   */
  @SuppressWarnings("JavaDoc") public Response healthcheckThrowing()
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException {
    PolicyBackoff backoff = ExponentialBackoff.newBuilder()
        .initialInterval(900, TimeUnit.MILLISECONDS)
        .maxAttempts(5)
        .maxInterval(6000, TimeUnit.MILLISECONDS)
        .build();
    Resource resource = client.resourceProvider().newResource();
    return resource.requestRetryThrowing("GET",
        UriBuilder.builder(client.baseURI()).path("health").buildString(),
        ResourceSupport.options("*/*").tokenProvider(client.resourceTokenProvider()),
        Response.class, backoff);
  }
}
