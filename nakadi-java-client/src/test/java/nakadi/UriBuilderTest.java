package nakadi;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UriBuilderTest {

  @Test
  public void buildPathBaseURIWithTrailingSlash() {
    assertEquals("http://example.org/event-types/one",
        UriBuilder.builder("http://example.org/")
            .path("event-types")
            .path("one")
            .buildString());
  }

  @Test
  public void buildPath() {
    assertEquals("http://example.org/event-types/one",
        UriBuilder.builder("http://example.org")
            .path("event-types")
            .path("one")
            .buildString());
  }

  @Test
  public void buildQuery() {
    assertEquals("http://example.org?a=1&b=2&c=3",
        UriBuilder.builder("http://example.org")
            .query("a", "1")
            .query("b", "2")
            .query("c", "3")
            .buildString());
  }

  @Test
  public void buildMultiQuery() {
    // test is fragile on param order in expectation
    assertEquals(
        "http://example.org?event_type=a&event_type=b&event_type=c&offset=3&limit=2&owning_application=acme",
        UriBuilder.builder("http://example.org")
            .query("event_type", "a", "b", "c")
            .query("limit", "2")
            .query("offset", "3")
            .query("owning_application", "acme")
            .buildString());
  }

  @Test
  public void buildMultiQueryReplacing() {
    assertEquals("http://example.org?a=1&b=2&c=3&offset=20&limit=10",
        UriBuilder.builder("http://example.org?limit=1&offset=2")
            .query("a", "1")
            .query("b", "2")
            .query("c", "3")
            .queryReplacing("limit", "10")
            .queryReplacing("offset", "20")
            .buildString());
  }

  @Test
  public void buildMultiQueryWithQueryParams() {
    QueryParams qp = new QueryParams()
        .param("event_type", "a", "b", "c")
        .param("limit", "2")
        .param("offset", "3")
        .param("owning_application", "acme");
    // test is fragile on param order in expectation
    assertEquals(
        "http://example.org?event_type=a&event_type=b&event_type=c&offset=3&limit=2&owning_application=acme",
        UriBuilder.builder("http://example.org")
            .query(qp)
            .buildString());
  }

  @Test
  public void buildMultiQueryWithQueryParamsReplacing() {
    QueryParams qp = new QueryParams()
        .param("event_type", "a", "b", "c")
        .paramReplacing("limit", "20")
        .paramReplacing("offset", "10")
        .param("owning_application", "acme");
    // todo: test is fragile on param order in expectation, reparse
    assertEquals(
        "http://example.org?event_type=a&event_type=b&event_type=c&offset=10&limit=20&owning_application=acme",
        UriBuilder.builder("http://example.org?offset=1&limit=2")
            .query(qp)
            .buildString());
  }

  @Test
  public void buildPathAndQuery() {
    assertEquals("http://example.org/event-types/one?a=1&b=2&c=3",
        UriBuilder.builder("http://example.org")
            .path("event-types")
            .path("one")
            .query("a", "1")
            .query("b", "2")
            .query("c", "3")
            .buildString());
  }
}
