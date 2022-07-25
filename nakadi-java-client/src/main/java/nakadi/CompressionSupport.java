package nakadi;

import java.io.IOException;
/**
 * Provides compression support for the client.
 */
public interface CompressionSupport {

  /**
   * Compress the supplied bytes
   *
   * @param bytes the target to compress
   * @return the compressed bytes
   */
  public byte[] compress(byte[] bytes);

  /**
   * Compress the supplied String
   *
   * @param json the target to compress
   * @return the compressed bytes
   */
  byte[] compress(String json);

  /**
   * Decompress the supplied array.
   *
   * @param compressed the target to de compress
   * @return the decompressed String
   */
  String decompress(byte[] compressed);

  /**
   * The name of the compression algorithm, suitable for use in the 'Content-Encoding' header.
   * @return the algorithm name.
   */
  String name();


}
