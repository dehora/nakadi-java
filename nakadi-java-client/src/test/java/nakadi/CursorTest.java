package nakadi;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class CursorTest {

  @Test
  public void shiftSerializationSkipsNullShifts() {
    final Cursor c = new Cursor().partition("0").offset("000000000000000025");
    final GsonSupport gson = new GsonSupport();
    final String json = gson.toJson(c);
    assertFalse("expecting unset shifts to not be serialized", json.contains("shift"));
  }
}