package nakadi;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StreamProcessorTest {

  private final NakadiClient client =
      NakadiClient.newBuilder().baseURI("http://localhost:9080").build();
  private StreamProcessor processor;

  @Before
  public void before() {
    processor = new StreamProcessor(client);
  }

  @Test
  public void testOffsetObservers() {

    StreamProcessor sp = StreamProcessor.newBuilder(client)
        .streamConfiguration(new StreamConfiguration().subscriptionId("s1"))
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    assertTrue(sp.streamOffsetObserver() instanceof SubscriptionOffsetObserver);

    sp = StreamProcessor.newBuilder(client)
        .streamConfiguration(new StreamConfiguration().eventTypeName("e1"))
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    assertTrue(sp.streamOffsetObserver() instanceof LoggingStreamOffsetObserver);
  }

  @Test
  public void testBuilder() {

    try {
      StreamProcessor.newBuilder(client).build();
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Please provide a stream configuration", e.getMessage());
    }

    try {
      StreamProcessor.newBuilder(client).streamConfiguration(new StreamConfiguration()).build();
      fail();
    } catch (NakadiException e) {
      assertEquals("Please supply either a subscription id or an event type;  (400)",
          e.getMessage());
    }

    try {
      StreamProcessor.newBuilder(client)
          .streamConfiguration(new StreamConfiguration().eventTypeName("et").subscriptionId("s1"))
          .build();
      fail();
    } catch (NakadiException e) {
      assertEquals(
          "Cannot be configured with both a subscription id or an event type; subscriptionId=s1 eventTypeName=et (400)",
          e.getMessage());
    }

    try {
      StreamProcessor.newBuilder(client)
          .streamConfiguration(new StreamConfiguration().eventTypeName("et"))
          .build();
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Please provide an observer factory", e.getMessage());
    }

    final StreamProcessor sp = StreamProcessor.newBuilder(client)
        .streamConfiguration(new StreamConfiguration().eventTypeName("et"))
        .streamObserverFactory(new LoggingStreamObserverProvider())
        .build();

    assertNotNull(sp);
  }
}