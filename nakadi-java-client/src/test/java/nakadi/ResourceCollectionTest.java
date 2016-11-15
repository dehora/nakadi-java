package nakadi;

import com.google.common.collect.Lists;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.After;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResourceCollectionTest {

  private static String lastItem = "3-3";
  private static String lastItemFirstPage = "1-3";
  private static ArrayList<String> itemsOne = Lists.newArrayList("1-1", "1-2", lastItemFirstPage);
  private static ArrayList<String> itemsTwo = Lists.newArrayList("2-1", "2-2", "2-3");
  private static ArrayList<String> itemsThree = Lists.newArrayList("3-1", "3-2", lastItem);
  private static ArrayList<String> itemsAll = Lists.newArrayList();

  static {
    itemsAll.addAll(itemsOne);
    itemsAll.addAll(itemsTwo);
    itemsAll.addAll(itemsThree);
  }

  @After
  public void after() {
    StringCollection.reset();
  }

  @Test
  public void testEmptyPage() {
    final ArrayList<String> strings = Lists.newArrayList();
    final Iterator<String> iterator = buildEmptyPage().iterable().iterator();
    //noinspection WhileLoopReplaceableByForEach
    while (iterator.hasNext()) {
      String next = iterator.next();
      strings.add(next);
    }
    assertTrue(strings.size() == 0);
    assertFalse(StringCollection.twoCalled);
    assertFalse(StringCollection.threeCalled);
    assertFalse(StringCollection.fetchPageCalled);
  }

  @Test
  public void testOnePage() {
    final ArrayList<String> strings = Lists.newArrayList();
    buildFirstPageOnly().iterable().forEach(strings::add);
    assertTrue(strings.size() == itemsOne.size());
    assertEquals(lastItemFirstPage, strings.get(strings.size() - 1));
    assertEquals(itemsOne, strings);
    assertFalse(StringCollection.twoCalled);
    assertFalse(StringCollection.threeCalled);
  }

  @Test
  public void testJankPage() {
    URI pageTwo = URI.create("http://localhost/strings/2");
    List<ResourceLink> linksOne = Lists.newArrayList(new ResourceLink("next", pageTwo));
    final ArrayList<String> strings = Lists.newArrayList();
    // the first page is empty but there is a next link ¯\_(ツ)_/¯
    final StringCollection jankCollection = new StringCollection(Lists.newArrayList(), linksOne);
    jankCollection.iterable().forEach(strings::add);
    final ArrayList<String> expected = Lists.newArrayList();
    expected.addAll(itemsTwo);
    expected.addAll(itemsThree);
    assertTrue(strings.size() == expected.size());
    assertEquals(lastItem, strings.get(strings.size() - 1));
    assertEquals(expected, strings);
    assertTrue(StringCollection.twoCalled);
    assertTrue(StringCollection.threeCalled);
  }

  @Test
  public void testIterator() {
    final ArrayList<String> strings = Lists.newArrayList();
    final Iterator<String> iterator = buildFirstPage().iterable().iterator();
    //noinspection WhileLoopReplaceableByForEach
    while (iterator.hasNext()) {
      String next = iterator.next();
      strings.add(next);
    }
    assertResults(strings);
  }

  @Test
  public void testForLoop() {
    final ArrayList<String> strings = Lists.newArrayList();
    for (String next : buildFirstPage().iterable()) {
      strings.add(next);
    }
    assertResults(strings);
  }

  @Test
  public void testIterable() {
    final ArrayList<String> strings = Lists.newArrayList();
    buildFirstPage().iterable().forEach(strings::add);
    assertResults(strings);
  }

  private void assertResults(ArrayList<String> strings) {
    assertTrue(strings.size() == itemsAll.size());
    assertEquals(lastItem, strings.get(strings.size() - 1));
    assertEquals(itemsAll, strings);
    assertTrue(StringCollection.twoCalled);
    assertTrue(StringCollection.threeCalled);
  }

  private StringCollection buildEmptyPage() {
    return new StringCollection(Lists.newArrayList(), Lists.newArrayList());
  }

  private StringCollection buildFirstPageOnly() {
    return new StringCollection(itemsOne, Lists.newArrayList());
  }

  private StringCollection buildFirstPage() {
    URI pageTwo = URI.create("http://localhost/strings/2");
    List<ResourceLink> linksOne = Lists.newArrayList(new ResourceLink("next", pageTwo));
    return new StringCollection(itemsOne, linksOne);
  }

  private static class StringCollection extends ResourceCollection<String> {

    static volatile boolean twoCalled = false;
    static volatile boolean threeCalled = false;
    static volatile boolean fetchPageCalled = false;

    StringCollection(List<String> items,
        List<ResourceLink> links) {
      super(items, links);
    }

    static void reset() {
      twoCalled = false;
      threeCalled = false;
      fetchPageCalled = false;
    }

    @Override public ResourceCollection<String> fetchPage(String url) {

      if (url.endsWith("2")) {
        twoCalled = true;
        URI pageThree = URI.create("http://localhost/strings/3");
        List<ResourceLink> linksOnPageTwo = Lists.newArrayList(new ResourceLink("next", pageThree));
        return new StringCollection(itemsTwo, linksOnPageTwo);
      }

      if (url.endsWith("3")) {
        threeCalled = true;
        List<ResourceLink> linksOnPageThree = Lists.newArrayList(); // no next page from here
        return new StringCollection(itemsThree, linksOnPageThree);
      }

      // if we get here, iteration's broken
      fail("Should not iterate past a collection with no next rel");

      return null;
    }
  }
}