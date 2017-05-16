package nakadi;

import java.io.Reader;
import java.lang.reflect.Type;

/**
 * Provides JSON support for the client.
 */
public interface JsonSupport {

  /**
   * Convert the object to a JSON String removing newlines and whitespace.
   *
   * @param o the target object
   * @return a JSON String with newlines and whitespace removed.
   */
  String toJsonCompressed(Object o);

  /**
   * Convert the object to a JSON String.
   *
   * @param o the target object
   * @return a JSON String.
   */
  String toJson(Object o);

  /**
   * Marshal the JSON data to an instance of T.
   *
   * @param raw JSON as a String
   * @param c the target class
   * @param <T> the parameterized target type
   * @return an instance of T
   */
  <T> T fromJson(String raw, Class<T> c);

  /**
   * Marshal the JSON data to an instance of T.
   *
   * @param raw JSON as a String
   * @param tType the type of the target
   * @param <T> the parameterized target type
   * @return an instance of T
   */
  <T> T fromJson(String raw, Type tType);

  /**
   * Marshal the JSON data to an instance of T.
   *
   * @param r JSON as a Reader
   * @param c the target class
   * @param <T> the parameterized target type
   * @return an instance of T
   */
  <T> T fromJson(Reader r, Class<T> c);

  /**
   * Marshal the JSON data to an instance of T.
   *
   * @param r JSON as a Reader
   * @param tType the type of the target
   * @param <T> the parameterized target type
   * @return an instance of T
   */
  <T> T fromJson(Reader r, Type tType);
}
