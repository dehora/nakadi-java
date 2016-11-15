package nakadi;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Allow  a generic type {@code T} to be passed around, avoiding erasure. <p> In this API it's used
 * to allow clients to declare the type used to marshal a batch's events. Clients have to create an
 * empty subclass, eg: <p>
 * <pre>
 * TypeLiteral&lt;List&lt;String&gt;&gt; literal = new TypeLiteral&lt;Map&lt;Integer,
 * String&gt;&gt;() {};
 * Type caught = literal.type();
 * </pre>
 * Based on Guice's TypeLiteral but far less sophisticated. <p>
 *
 * @param <T> The generic type to capture
 */
public class TypeLiteral<T> {

  private final Type type;

  public TypeLiteral() {
    final Class<? extends TypeLiteral> sClass = getClass();
    final Type gSuper = sClass.getGenericSuperclass();
    if (gSuper instanceof Class) {
      throw new IllegalArgumentException("No type parameter for " + ((Class) gSuper).getName());
    }
    final ParameterizedType pType = (ParameterizedType) gSuper;
    type = pType.getActualTypeArguments()[0];
  }

  public Type type() {
    return type;
  }

  @Override public final int hashCode() {
    return type.hashCode();
  }

  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  @Override
  public boolean equals(Object o) {
    return o != null && (this == o || type.equals(((TypeLiteral) o).type));
  }
}
