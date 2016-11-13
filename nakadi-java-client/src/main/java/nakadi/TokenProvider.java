package nakadi;

import java.util.Optional;

/**
 * Supplied to {@link NakadiClient} and provides a token string that can be used
 * to authorize requests against the server. Acts as a {@link FunctionalInterface} and
 * can be supplied as a lambda expression.
 */
@FunctionalInterface
public interface TokenProvider {

  String NAKADI_CONFIG_WRITE = "nakadi.config.write";
  String NAKADI_EVENT_TYPE_WRITE = "nakadi.event_type.write";
  String NAKADI_EVENT_STREAM_WRITE = "nakadi.event_stream.write";
  String NAKADI_EVENT_STREAM_READ = "nakadi.event_stream.read";
  String UID = "uid";

  /**
   * @return a value suitable for use in an Authorization header, or null to suppress the
   * Authorization header being set
   */
  Optional<String> authHeaderValue(String scope);

}
