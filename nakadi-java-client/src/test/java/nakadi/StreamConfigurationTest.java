package nakadi;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamConfigurationTest {

  private final NakadiClient client =
      NakadiClient.newBuilder().baseURI("http://localhost:9080").build();

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