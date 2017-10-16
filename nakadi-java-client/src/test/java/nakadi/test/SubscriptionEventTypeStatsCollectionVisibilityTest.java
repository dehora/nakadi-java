package nakadi.test;

import com.google.common.collect.Lists;
import nakadi.SubscriptionEventTypeStatsCollection;
import org.junit.Test;

import static org.junit.Assert.*;

public class SubscriptionEventTypeStatsCollectionVisibilityTest {

  @Test
  public void isPublic() {

    assertNotNull(new SubscriptionEventTypeStatsCollection(
        null,
        Lists.newArrayList(),
        null,
        null));
  }

}
