package nakadi;

import org.junit.Test;

public class GsonSupportTest {

  @Test
  public void string() {
    GsonSupport gson = new GsonSupport();

    TypeLiteral<String> typeLiteral = new TypeLiteral<String>() {
    };

    String json =
        "{\"metadata\":{\"occurred_at\":\"2016-09-20T21:52:00Z\",\"eid\":\"a2ab0b7c-ee58-48e5-b96a-d13bce73d857\",\"event_type\":\"et-1\",\"partition\":\"0\",\"received_at\":\"2016-10-26T20:54:41.300Z\",\"flow_id\":\"nP1I7tHXBwICOh5HqLnICOPh\"},\"data_op\":\"C\",\"data\":{\"id\":\"1\"},\"data_type\":\"et-1\"}";

    gson.fromJson(json, String.class);
    gson.fromJson(json, typeLiteral.type());
  }
}