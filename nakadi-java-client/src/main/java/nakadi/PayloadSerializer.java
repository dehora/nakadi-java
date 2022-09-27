package nakadi;

import java.util.List;

public interface PayloadSerializer {

    <T> byte[] toBytes(List<T> o);

    <T> Object transformEventRecord(EventRecord<T> eventRecord);

    String payloadMimeType();
}
