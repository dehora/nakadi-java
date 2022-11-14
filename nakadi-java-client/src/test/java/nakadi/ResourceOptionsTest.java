package nakadi;

import junit.framework.TestCase;

public class ResourceOptionsTest extends TestCase {

  public void testContentType() {

    ResourceOptions options = new ResourceOptions();
    String ctype = "application/foo";
    options.contentType(ctype);
    assertEquals(options.headers().get(ResourceOptions.HEADER_CONTENT_TYPE), ctype);
  }

  public void testAccept() {

    ResourceOptions options = new ResourceOptions();
    String accept = "application/bar";
    options.accept(accept);
    assertEquals(options.headers().get(ResourceOptions.HEADER_ACCEPT), accept);
  }

  public void testAcceptCharset() {

    ResourceOptions options = new ResourceOptions();
    String cset = "UTF-9";
    options.acceptCharset(cset);
    assertEquals(options.headers().get(ResourceOptions.HEADER_ACCEPT_CHARSET), cset);
  }
}
