package nakadi;

import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class CompressionSupportTest {

  @Test
  public void compressThenUncompress() throws IOException  {

    final CompressionSupportGzip support = new CompressionSupportGzip();

    String json =
        "{\"metadata\":{\"occurred_at\":\"2016-09-20T21:52:00Z\",\"eid\":\"a2ab0b7c-ee58-48e5-b96a-d13bce73d857\",\"event_type\":\"et-1\",\"partition\":\"0\",\"received_at\":\"2016-10-26T20:54:41.300Z\",\"flow_id\":\"nP1I7tHXBwICOh5HqLnICOPh\"},\"data_op\":\"C\",\"data\":{\"id\":\"1\"},\"data_type\":\"et-1\"}";

    final byte[] zipped = support.toGzip(json);
    final String unzipped = support.fromGzip(zipped);
    assertEquals(json, unzipped);
  }

}