package nakadi;

interface StreamProcessorManaged {

  void start() throws IllegalStateException;

  void stop();

  boolean running();

  void retryAttemptsFinished(boolean completed);

  void failedProcessorException(Throwable t);

}
