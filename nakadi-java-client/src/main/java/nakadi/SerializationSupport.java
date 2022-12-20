package nakadi;

import java.util.Collection;

public interface SerializationSupport {

    <T> byte[] serializePayload(NakadiClient client, String eventTypeName, Collection<T> events);

    String contentType();

}
