package nakadi;

import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamConfigurationTest {

  private final NakadiClient client =
      NakadiClient.newBuilder().baseURI("http://localhost:9080").build();

  @Test
  public void maxRetryDelay() {
    StreamConfiguration et = new StreamConfiguration();
    try {
      et.maxRetryDelay(
          TimeUnit.SECONDS.toMillis(StreamConnectionRetry.DEFAULT_MIN_DELAY_SECONDS) - 1,
          TimeUnit.MILLISECONDS);
      fail("did not throw on very low max delay");
    } catch (IllegalArgumentException ignored) {
    }

    et.maxRetryDelay(TimeUnit.SECONDS.toMillis(StreamConnectionRetry.DEFAULT_MIN_DELAY_SECONDS),
        TimeUnit.MILLISECONDS);
    et.maxRetryDelay(TimeUnit.SECONDS.toMillis(StreamConnectionRetry.DEFAULT_MIN_DELAY_SECONDS) + 1,
        TimeUnit.MILLISECONDS);
  }

  @Test
  public void testTypeCheck() {

    StreamConfiguration et = new StreamConfiguration().eventTypeName("et");
    assertFalse(et.isSubscriptionStream());
    assertTrue(et.isEventTypeStream());

    StreamConfiguration s1 = new StreamConfiguration().subscriptionId("s1");
    assertTrue(s1.isSubscriptionStream());
    assertFalse(s1.isEventTypeStream());
  }
}