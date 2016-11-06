package nakadi;

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NakadiClientTest {

  @Test
  public void testBuilderJson() {
    final NakadiClient client = TestSupport.newNakadiClient();
    assertTrue(client.jsonSupport() != null);

    final String event = TestSupport.load("batch-1.json");
    final Map map = client.jsonSupport().fromJson(event, Map.class);
    assertTrue(map.containsKey("cursor"));
    assertTrue(map.containsKey("events"));
    assertTrue(((List) map.get("events")).size() == 2);
  }

  @Test
  public void testBuilderResourceProvider() {
    assertTrue(TestSupport.newNakadiClient().resourceProvider() != null);
  }

  @Test
  public void testBuilderResourceTokenProvider() {
    assertTrue(TestSupport.newNakadiClient().resourceTokenProvider() != null);
  }

  @Test
  public void testBuilderUri() {
    try {
      NakadiClient.newBuilder().build();
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(true);
    }

    try {
      NakadiClient.newBuilder().baseURI("http://example .com").build();
      fail();
    } catch (NakadiException e) {
      assertTrue(true);
    }

    try {
      NakadiClient.newBuilder().baseURI("http://example.com").build();
    } catch (NakadiException e) {
      e.printStackTrace();
      fail();
    }

    try {
      NakadiClient.newBuilder().baseURI(new URI("http://example.com")).build();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
