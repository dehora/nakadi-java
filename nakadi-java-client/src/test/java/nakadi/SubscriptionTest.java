package nakadi;

import com.google.common.collect.Lists;
import java.util.List;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SubscriptionTest {

  @Test
  public void testCursors() {
    Subscription subscription = new Subscription();

    try {
      subscription.initialCursors(null);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    try {
      subscription.initialCursors(Lists.newArrayList());
      fail();
    } catch (IllegalArgumentException ignored) {
    }


    Cursor cursor = new Cursor("p", "o");
    try {
      subscription.initialCursors(Lists.newArrayList(cursor));
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    cursor = new Cursor("p", "o", "e", "t");

    subscription.initialCursors(Lists.newArrayList(cursor));
    List<Cursor> cursors = subscription.initialCursors();
    assertTrue(cursors.size() == 1);
    assertFalse(cursors.get(0).cursorToken().isPresent());
    assertEquals("p", cursors.get(0).partition());
    assertEquals("o", cursors.get(0).offset());
    assertEquals("e", cursors.get(0).eventType().get());

  }
}