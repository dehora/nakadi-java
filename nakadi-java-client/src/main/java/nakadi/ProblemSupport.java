package nakadi;

import java.util.Optional;

class ProblemSupport {

  static Problem toProblem(Response response, JsonSupport jsonSupport) {
    final String raw = response.responseBody().asString();
    Problem problem = deserializeProblem(response.statusCode(), raw, jsonSupport);

    if (problem == null) {
      problem = Problem.noProblemo("no problem sent back from server", "", response.statusCode());
    }

    // workaround for https://github.com/zalando/nakadi/issues/645

    if (problem.status() == 0 && (response.statusCode() == 400 || response.statusCode() == 401)) {

      if (raw.contains("\"error_description\"")) {
        problem = Problem.authProblem("token_assumed_unauthorized",
            "Inferred from invalid response there was an auth issue, "
                + "see https://github.com/zalando/nakadi/issues/645 raw_response=" + raw);
      }
    }

    problem = Optional.ofNullable(problem)
        .orElse(Problem.noProblemo("no problem sent back from server", "", response.statusCode()));

    return problem;
  }

  static Problem deserializeProblem(int statusCode, String body, JsonSupport jsonSupport) {
    try {
      return jsonSupport.fromJson(body, Problem.class);
    } catch(Exception e) {
      return Problem.rawProblem(statusCode, body, e.getMessage());
    }
  }

  static <T> T throwProblem(int code, Problem problem, MetricCollector metricCollector) {
    if (code == 401) {
      metricCollector.mark(MetricCollector.Meter.http401);
      throw new AuthorizationException(problem);
    } else if (code == 403) {
      metricCollector.mark(MetricCollector.Meter.http403);
      throw new AuthorizationException(problem);
    } else if (code == 404 || code == 410) {
      metricCollector.mark(MetricCollector.Meter.http404);
      throw new NotFoundException(problem);
    } else if (code == 409) {
      metricCollector.mark(MetricCollector.Meter.http409);
      throw new ConflictException(problem);
    } else if (code == 412) {
      metricCollector.mark(MetricCollector.Meter.http412);
      // eg bad cursors: Precondition Failed; offset 98 for partition 0 is unavailable (412)
      throw new PreconditionFailedException(problem);
    } else if (code == 422) {
      metricCollector.mark(MetricCollector.Meter.http422);
      throw new InvalidException(problem);
    } else if (code == 429) {
      metricCollector.mark(MetricCollector.Meter.http429);
      throw new RateLimitException(problem);
    } else if (code == 400) {
      metricCollector.mark(MetricCollector.Meter.http400);
      throw new ClientException(problem);
    } else if (code >= 400 && code < 500) {
      metricCollector.mark(MetricCollector.Meter.http4xx);
      throw new ClientException(problem);
    } else if (code == 500) {
      metricCollector.mark(MetricCollector.Meter.http500);
      throw new ServerException(problem);
    } else if (code == 503) {
      metricCollector.mark(MetricCollector.Meter.http503);
      throw new ServerException(problem);
    } else if (code > 500 && code < 600) {
      metricCollector.mark(MetricCollector.Meter.http5xx);
      throw new ServerException(problem);
    } else {
      metricCollector.mark(MetricCollector.Meter.httpUnknown);
      throw new HttpException(problem);
    }
  }

}
