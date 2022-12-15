package nakadi;

import java.util.Collection;

public interface PublishingBatchSerializer {

    <T> byte[] toBytes(SerializationContext context, Collection<T> events);

}
