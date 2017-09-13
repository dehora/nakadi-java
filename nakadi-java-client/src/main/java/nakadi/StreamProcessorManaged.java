package nakadi;

interface StreamProcessorManaged {

  void start() throws IllegalStateException;

  void stop();

  boolean disposed();

  boolean disposing();
}
