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
          .put(new Throwable(""), true)
          .put(new Exception(), true)
          .put(new InterruptedException(), true)
          .put(new IOException(), true)
          .put(new NakadiException(Problem.localProblem("", "")), true)
          .put(new ClientException(Problem.localProblem("", "")), true)
          .put(new ContractException(Problem.localProblem("", "")), true)
          .put(new NotFoundException(Problem.localProblem("", "")), true)
          .put(new PreconditionFailedException(Problem.localProblem("", "")), true)
          // nope
          .put(new NonRetryableNakadiException(Problem.localProblem("", "")), false)
          .put(new Error(), false)
          .build();

  @Test
  public void isApiRequestRetryable() {
    for (Map.Entry<Throwable, Boolean> entry : CODES_TO_EXCEPTIONS.entrySet()) {
      assertTrue(entry.getValue() == ExceptionSupport.isApiRequestRetryable(entry.getKey()));
    }
  }

  @Test
  public void isConsumerStreamRetryable() {
    for (Map.Entry<Throwable, Boolean> e : CODES_TO_EXCEPTIONS.entrySet()) {
      assertTrue(e.getValue() == ExceptionSupport.isConsumerStreamRetryable(e.getKey()));
    }
  }
}