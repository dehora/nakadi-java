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

class GzipSupport {

  public byte[] toGzip(String json) throws IOException {

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (final BufferedSink sink = Okio.buffer(new GzipSink(Okio.sink(baos)))) {
      sink.write(json.getBytes(Charsets.UTF_8));
    }

    return baos.toByteArray();
  }

  public String fromGzip(byte[] gzipped) throws IOException {

    try (final BufferedSource source = Okio.buffer(
        new GzipSource(Okio.source(new ByteArrayInputStream(gzipped))))) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(source.readByteArray());
      return baos.toString(Charsets.UTF_8.name());
    }
  }
}
