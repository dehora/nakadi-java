package nakadi;

import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TypeLiteralTest {

  @Test
  public void equals() {

    TypeLiteral<UndefinedEventMapped> a = new TypeLiteral<UndefinedEventMapped>() {
    };

    TypeLiteral<UndefinedEventMapped> b = new TypeLiteral<UndefinedEventMapped>() {
    };

    assertEquals(a, b);
    assertEquals(b, a);
  }

  @Test
  public void testDetectBusinessAndUndefinedEvent() {

    TypeLiteral<BusinessEventMapped> be = new TypeLiteral<BusinessEventMapped>() {
    };

    TypeLiteral<UndefinedEventMapped> ue = new TypeLiteral<UndefinedEventMapped>() {
    };

    assertEquals("nakadi.BusinessEventMapped", be.type().getTypeName());
    assertEquals("nakadi.UndefinedEventMapped", ue.type().getTypeName());
  }

  @Test
  public void testNoTypeParameter() {
    try {
      new TypeLiteral() {
      };
      fail();
    } catch (RuntimeException ignored) {
    }
  }

  @Test
  public void testParameterizedType() {
    TypeLiteral<List<String>> a = new TypeLiteral<List<String>>() {
    };
    TypeLiteral<List<String>> b = new TypeLiteral<List<String>>() {
    };
    assertEquals(a, b);
    assertEquals(b, a);
    assertEquals("java.util.List<java.lang.String>", a.type().getTypeName());
  }

  @Test
  public void testWilcardType() {
    TypeLiteral<List<?>> a = new TypeLiteral<List<?>>() {
    };
    TypeLiteral<List<?>> b = new TypeLiteral<List<?>>() {
    };
    assertEquals(a, b);
    assertEquals(b, a);
    assertEquals("java.util.List<?>", a.type().getTypeName());
  }

  @Test
  public void testWithWilcardParameterizedType() {
    TypeLiteral<List<? extends Map>> a = new TypeLiteral<List<? extends Map>>() {
    };
    TypeLiteral<List<? extends Map>> b = new TypeLiteral<List<? extends Map>>() {
    };
    assertEquals(a, b);
    assertEquals(b, a);
    assertEquals("java.util.List<? extends java.util.Map>", a.type().getTypeName());
  }

  @Test
  public void testWithWilcardParameterizedTypeCthulhu() {
    TypeLiteral<List<? extends Map<String, List<String>>>> a =
        new TypeLiteral<List<? extends Map<String, List<String>>>>() {
        };
    TypeLiteral<List<? extends Map<String, List<String>>>> b =
        new TypeLiteral<List<? extends Map<String, List<String>>>>() {
        };
    assertEquals(a, b);
    assertEquals(b, a);
    assertEquals(
        "java.util.List<? extends java.util.Map<java.lang.String, java.util.List<java.lang.String>>>",
        a.type().getTypeName());
  }
}