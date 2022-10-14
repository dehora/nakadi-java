package nakadi;

import java.util.Collection;

public interface PayloadSerializer {

    <T> byte[] toBytes(String eventTypeName, Collection<T> o);

    String payloadMimeType();
}
