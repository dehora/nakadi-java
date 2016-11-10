package nakadi;

/**
 * Interface for known event categories.
 *
 * <p>
 * In the API, only the DataChangeEvent has a data field; the other two are
 * effectively raw JSON objects with no place to bind custom data. However, in
 * the absence of a common top level structure for events and their payloads, this
 * client gives all three categories a data field and the implementation does
 * bespoke serdes with business and undefined categories to match their Open API
 * definitions. This allows us to support custom data uniformly while providing
 * useful types for undefined, business and data change categories.
 * </p>
 */
public interface Event<T> {

  /**
   * @return the custom data
   */
  T data();

}
