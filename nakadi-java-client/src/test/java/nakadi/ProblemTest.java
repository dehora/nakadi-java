package nakadi;

import org.junit.Test;

import static org.junit.Assert.*;

public class ProblemTest {

  @Test
  public void testMessageFieldExcluded() {
    final NakadiClient client = TestSupport.newNakadiClient();
    final Problem problem = Problem.localProblem("title", "detail");
    problem.toMessage();
    assertFalse(client.jsonSupport().toJson(problem).contains("message"));
  }
}