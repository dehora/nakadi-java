package nakadi;

import junit.framework.TestCase;

public class ResourceSupportTest extends TestCase {

  public void testOptions() {

    String accept = "application/bar";
    ResourceOptions options = ResourceSupport.options(accept);

    assertEquals(options.headers().get(ResourceOptions.HEADER_ACCEPT), accept);

    assertEquals(options.headers().get(ResourceOptions.HEADER_ACCEPT_CHARSET),
        "UTF-8");

    assertNull(options.headers().get(ResourceOptions.HEADER_CONTENT_TYPE));
  }

  public void testOptionsWithJsonContent() {

    String accept = "application/foo";
    ResourceOptions options = ResourceSupport.options(accept);

    assertEquals(options.headers().get(ResourceOptions.HEADER_ACCEPT), accept);

    assertEquals(options.headers().get(ResourceOptions.HEADER_ACCEPT_CHARSET),
        ResourceSupport.CHARSET_UTF_8);

    assertNull(options.headers().get(ResourceOptions.HEADER_CONTENT_TYPE));

    ResourceOptions options1 = ResourceSupport.optionsWithJsonContent(options);

    assertEquals(options1.headers().get(ResourceOptions.HEADER_CONTENT_TYPE),
        ResourceSupport.APPLICATION_JSON_CHARSET_UTF_8);
  }
}
