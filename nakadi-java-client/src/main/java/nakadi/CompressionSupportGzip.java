package nakadi;

import com.google.common.base.Charsets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import okio.BufferedSink;
import okio.BufferedSource;
import okio.GzipSink;
import okio.GzipSource;
import okio.Okio;

class CompressionSupportGzip implements CompressionSupport {

  @Override public byte[] compress(byte[] bytes) {
    try {
      return toGzip(bytes);
    } catch (IOException e) {
      throw new EncodingException(
          Problem.localProblem("could not gzip request entity", ""), e);
    }
  }

  @Override public byte[] compress(String json) {
    try {

      return toGzip(json);
    } catch (IOException e) {
      throw new EncodingException(
          Problem.localProblem("could not gzip request entity", ""), e);
    }
  }

  @Override public String decompress(byte[] compressed) {
    try {
      return fromGzip(compressed);
    } catch (IOException e) {
      throw new EncodingException(
          Problem.localProblem("could not gzip request entity", ""), e);
    }
  }

  @Override public String name() {
    return "gzip";
  }

  byte[] toGzip(byte[] json) throws IOException {

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (final BufferedSink sink = Okio.buffer(new GzipSink(Okio.sink(baos)))) {
      sink.write(json);
    }

    return baos.toByteArray();
  }

  byte[] toGzip(String json) throws IOException {

    return toGzip(json.getBytes(Charsets.UTF_8));
  }

  String fromGzip(byte[] gzipped) throws IOException {

    try (final BufferedSource source = Okio.buffer(
        new GzipSource(Okio.source(new ByteArrayInputStream(gzipped))))) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(source.readByteArray());
      return baos.toString(Charsets.UTF_8.name());
    }
  }
}
