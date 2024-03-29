package nakadi;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProblemSupportTest {

  private final GsonSupport jsonSupport = new GsonSupport();

  @Test
  public void toProblemForNakadi645() {

    // test workarounds for https://github.com/zalando/nakadi/issues/645

    Response response = buildReponse(TestSupport.load("nakadi-645-invalid-problem-401.json"), 401);
    Problem problem = ProblemSupport.toProblem(response, jsonSupport);
    assertEquals("token_assumed_unauthorized", problem.title());
    assertEquals(401, problem.status());

    response = buildReponse(TestSupport.load("nakadi-645-invalid-problem-400.json"), 400);
    problem = ProblemSupport.toProblem(response, jsonSupport);
    assertEquals("token_assumed_unauthorized", problem.title());
    // check we map the 400 onto 401
    assertEquals(401, problem.status());
  }

  @Test
  public void toProblemForNakadi357() {

    // test fix for https://github.com/dehora/nakadi-java/issues/357

    Response response = buildReponse("non-json-payload", 400);
    Problem problem = ProblemSupport.toProblem(response, jsonSupport);
    assertEquals("non-json-payload", problem.title());
    assertEquals(400, problem.status());
    assertEquals(Optional.of("java.lang.IllegalStateException: Expected BEGIN_OBJECT but was STRING at line 1 column 1 path $"), problem.detail());
  }

  private Response buildReponse(String json, int code) {
    return new Response() {
      @Override public int statusCode() {
        return code;
      }

      @Override public String reason() {
        return "401 Unauthorized";
      }

      @Override public Map<String, List<String>> headers() {
        return null;
      }

      @Override public void close() {
        try {
          responseBody().close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      @Override public ResponseBody responseBody() {
        return new ResponseBody() {
          @Override public String asString() throws ContractRetryableException {
            return json;
          }

          @Override public Reader asReader() {
            return null;
          }

          @Override public InputStream asInputStream() {
            return null;
          }

          @Override public String mediaTypeString() {
            return null;
          }

          @Override public long contentLength() {
            return 0;
          }

          @Override public void close() throws IOException {

          }
        };
      }
    };
  }
}
