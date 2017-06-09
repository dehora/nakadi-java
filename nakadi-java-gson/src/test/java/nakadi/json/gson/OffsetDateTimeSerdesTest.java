package nakadi.json.gson;

import org.junit.Test;

import static org.junit.Assert.fail;

public class OffsetDateTimeSerdesTest {

  @Test
  public void testSerdes() {
    OffsetDateTimeSerdes serdes = new OffsetDateTimeSerdes();
    try {
      // https://validator.w3.org/feed/docs/error/InvalidRFC3339Date.html
      serdes.toOffsetDateTime("2002-10-02T10:00:00-05:00");
      serdes.toOffsetDateTime("2002-10-02T15:00:00Z");
      serdes.toOffsetDateTime("2002-10-02T15:00:00.05Z");
      // https://tools.ietf.org/html/rfc3339
      serdes.toOffsetDateTime("1937-01-01T12:00:27.87+00:20");
      serdes.toOffsetDateTime("1996-12-19T16:39:57-08:00");
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

    try {
      /*
      this date is from https://tools.ietf.org/html/rfc3339 and
      represents a leap second (note the 60 at the end). Our default
      iso formatter doesn't handle this so it adding it here as a memo
      that we cater for it
       */
      serdes.toOffsetDateTime("2016-12-31T23:59:60Z");
    } catch (Exception ignored) {
      fail();
    }
  }
}
