package nakadi;

import com.google.common.collect.ImmutableMap;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class ExceptionSupportTest {

  private static final ImmutableMap<Throwable, Boolean> CODES_TO_EXCEPTIONS =
      ImmutableMap.<Throwable, Boolean>builder()
          // ok
          .put(new ServerException(Problem.localProblem("", "")), true)
          .put(new NetworkException(Problem.localProblem("", "")), true)
          .put(new RateLimitException(Problem.localProblem("", "")), true)
          .put(new EOFException(), true)
          .put(new UncheckedIOException(new java.net.SocketTimeoutException()), true)
          .put(new UncheckedIOException(new IOException("")), true)
          .put(new java.util.concurrent.TimeoutException(), true)
          // nope
          .put(new Throwable(""), false)
          .put(new Exception(), false)
          .put(new InterruptedException(), false)
          .put(new IOException(), false)
          .put(new NakadiException(Problem.localProblem("", "")), false)
          .put(new ClientException(Problem.localProblem("", "")), false)
          .put(new ContractException(Problem.localProblem("", "")), false)
          .put(new NotFoundException(Problem.localProblem("", "")), false)
          .put(new PreconditionFailedException(Problem.localProblem("", "")), false)
          .build();

  @Test
  public void isEventStreamRetryable() {
    for (Map.Entry<Throwable, Boolean> entry : CODES_TO_EXCEPTIONS.entrySet()) {
      assertTrue(entry.getValue() == ExceptionSupport.isEventStreamRetryable(entry.getKey()));
      assertFalse(ExceptionSupport.isEventStreamRetryable(
          new ConflictException(Problem.localProblem("", ""))));
    }
  }

  @Test
  public void isSubscriptionStreamRetryable() {
    for (Map.Entry<Throwable, Boolean> e : CODES_TO_EXCEPTIONS.entrySet()) {
      assertTrue(e.getValue() == ExceptionSupport.isSubscriptionStreamRetryable(e.getKey()));
      assertTrue(ExceptionSupport.isSubscriptionStreamRetryable(
          new ConflictException(Problem.localProblem("", ""))));
    }
  }
}