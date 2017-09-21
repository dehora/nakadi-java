package nakadi.token.zign;

import org.junit.Test;

import static org.junit.Assert.*;

public class TokenProviderZignTest {

  @Test
  public void builder() {
    try {
      TokenProviderZign.newBuilder().refreshEvery(1, null);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    try {
      //noinspection ConfusingArgumentToVarargsMethod
      TokenProviderZign.newBuilder().scopes(null);
    } catch (IllegalArgumentException ignored) {
      fail();
    }

    try {
      TokenProviderZign.newBuilder().waitFor(1, null);
      fail();
    } catch (IllegalArgumentException ignored) {
    }
  }

}
