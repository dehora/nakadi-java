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
      TokenProviderZign.newBuilder().scopes(null);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    try {
      TokenProviderZign.newBuilder().waitFor(1, null);
      fail();
    } catch (IllegalArgumentException ignored) {
    }
  }

}