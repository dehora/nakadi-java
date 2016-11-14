package nakadi;

import java.util.concurrent.TimeUnit;

/**
 * Resource provides a purpose built abstraction over the HTTP implementation used by the API.
 */
public interface Resource {

  String GET = "GET";
  String DELETE = "DELETE";
  String HEAD = "HEAD";
  String POST = "POST";
  String PUT = "PUT";

  /**
   * Sets the default connect timeout for new connections. If 0, no timeout.
   */
  Resource connectTimeout(long timeout, TimeUnit unit);

  /**
   * Sets the default read timeout for connections. If 0, no timeout.
   */
  Resource readTimeout(long timeout, TimeUnit unit);

  Resource policyBackoff(RetryPolicy retryPolicy);

  // todo: reduce these methods and figure out when to throw exceptions

  /**
   * Make a request against the server without a request entity. Useful for get/delete/head
   * requests. Exceptions are not thrown for HTTP level errors (4xx and 5xx) so callers will
   * need to inspect the Response to see how things went.
   *
   * @param method the http method
   * @param url the resource url
   * @param options requestThrowing options such as headers, and tokens.
   * @return a Response
   * @throws NakadiException an exception not related to a HTTP response error
   */
  Response request(String method, String url, ResourceOptions options) throws NakadiException;

  /**
   * Make a request against the server with a request entity. Useful for post
   * requests. Exceptions are not thrown for HTTP level errors (4xx and 5xx) so callers will
   * need to inspect the Response to see how things went.
   *
   * The serialization assumes the output is JSON for now which is ok for the current API.
   *
   * @param method the http method
   * @param url the resource url
   * @param options requestThrowing options such as headers, and tokens.
   * @param body the object to create a JSON requestThrowing body from
   * @return a http response
   * @throws NakadiException a non HTTP based exception
   */
  <Req> Response request(String method, String url, ResourceOptions options, Req body)
      throws NakadiException;

  /**
   * Make a request against the server without a request entity. Useful for get/delete/head
   * requests. Exceptions are thrown for HTTP level errors (4xx and 5xx).
   *
   * @param method the http method
   * @param url the resource url
   * @param options requestThrowing options such as headers, and tokens.
   * @return a Response
   * @throws AuthorizationException for 401 and 403
   * @throws ClientException for general 4xx errors
   * @throws ServerException for general 5xx errors
   * @throws InvalidException for 422
   * @throws RateLimitException for 429
   * @throws NakadiException a non HTTP based exception
   */
  Response requestThrowing(String method, String url, ResourceOptions options)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * Make a request against the server with a request entity. Useful for post
   * requests. Exceptions are thrown for HTTP level errors (4xx and 5xx).
   *
   * The serialization assumes the output is JSON for now which is ok for the current API.
   *
   * @param method the http method
   * @param url the resource url
   * @param options requestThrowing options such as headers, and tokens.
   * @param body the object to create a JSON requestThrowing body from
   * @param <Req> the body type
   * @return a http response
   * @throws AuthorizationException for 401 and 403
   * @throws ClientException for general 4xx errors
   * @throws ServerException for general 5xx errors
   * @throws InvalidException for 422
   * @throws RateLimitException for 429
   * @throws NakadiException a non HTTP based exception
   */
  <Req> Response requestThrowing(String method, String url, ResourceOptions options, Req body)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

  /**
   * Make a request against the server with an expected response entity. Useful for get
   * requests. Exceptions are thrown for HTTP level errors (4xx and 5xx).
   *
   * The marshalling assumes the response body is JSON for now which is ok for the current API.
   *
   * @param method the http method
   * @param url the resource url
   * @param options requestThrowing options such as headers, and tokens.
   * @param res the class to marshal the response body.
   * @return a instance of the response class supplied
   * @throws AuthorizationException for 401 and 403
   * @throws ClientException for general 4xx errors
   * @throws ServerException for general 5xx errors
   * @throws InvalidException for 422
   * @throws RateLimitException for 429
   * @throws NakadiException a non HTTP based exception
   */
  <Res> Res requestThrowing(String method, String url, ResourceOptions options, Class<Res> res)
      throws AuthorizationException, ClientException, ServerException, InvalidException,
      RateLimitException, NakadiException;

}
