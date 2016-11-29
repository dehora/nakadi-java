package nakadi;

import java.util.Map;

/**
 * Convenience type literals for some common serdes options.
 */
public class TypeLiterals {

  /**
   * Represent an event as a String
   */
  public static final TypeLiteral<String> OF_STRING = new TypeLiteral<String>() {
  };

  /**
   * Represent an event as a typed Map
   */
  public static final TypeLiteral<Map<String, Object>> OF_MAP =
      new TypeLiteral<Map<String, Object>>() {
      };

  /**
   * Represent a {@link DataChangeEvent} with typed Map data.
   */
  public static final TypeLiteral<DataChangeEvent<Map<String, Object>>> OF_DATA_MAP =
      new TypeLiteral<DataChangeEvent<Map<String, Object>>>() {
      };

  /**
   * Represent a {@link BusinessEventMapped} with typed Map data.
   */
  public static final TypeLiteral<BusinessEventMapped<Map<String, Object>>> OF_BUSINESS_MAP =
      new TypeLiteral<BusinessEventMapped<Map<String, Object>>>() {
      };

  /**
   * Represent an {@link UndefinedEventMapped} with typed Map data.
   */
  public static final TypeLiteral<UndefinedEventMapped<Map<String, Object>>> OF_UNDEFINED_MAP =
      new TypeLiteral<UndefinedEventMapped<Map<String, Object>>>() {
      };

}
