package nakadi;

public interface Checkpointer {

  void checkpoint(StreamCursorContext context);

}
