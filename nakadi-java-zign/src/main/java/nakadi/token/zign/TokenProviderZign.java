package nakadi.token.zign;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import nakadi.ExecutorServiceSupport;
import nakadi.NakadiException;
import nakadi.TokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenProviderZign implements TokenProvider {

  private static final Logger logger =
      LoggerFactory.getLogger(TokenProviderZign.class.getSimpleName());
  private static final int REFRESH_EVERY_SECONDS = 120;
  private static final int WAIT_FOR_ZIGN_SECCONDS = 30;
  private static final String THREAD_NAME = "nakadi-java-zign";

  private final Set<String> scopes;
  private long refreshEvery = REFRESH_EVERY_SECONDS;
  private long waitFor = WAIT_FOR_ZIGN_SECCONDS;
  // not provided by builder
  private final AtomicBoolean started = new AtomicBoolean(false);
  private ScheduledExecutorService executorService;
  private Map<String, String> scopeTokens = new HashMap<>();

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override public Optional<String> authHeaderValue(String scope) {
    if (scopeTokens.containsKey(scope)) {
      return Optional.of("Bearer " + scopeTokens.get(scope));
    } else {
      logger.info("No token for scope={}, trying uid scope", scope);
      return Optional.of("Bearer " + scopeTokens.get(TokenProvider.UID));
    }
  }

  private TokenProviderZign(Builder builder) {
    this.scopes = builder.scopes;
    this.refreshEvery = builder.refreshEvery;
    this.waitFor = builder.waitFor;
  }

  public void start() {

    if (!started.getAndSet(true)) {
      loadTokens(); // first time run, then refresh in the background
      executorService = Executors.newSingleThreadScheduledExecutor();
      executorService.scheduleAtFixedRate(this::refreshTokens, refreshEvery, refreshEvery,
          TimeUnit.SECONDS);

      logger.info("refreshing tokens in background {}, every {} seconds", scopes, refreshEvery);
    }
  }

  void refreshTokens() {
    Thread.currentThread().setName(THREAD_NAME);
    logger.info("refreshing tokens {}", scopes);
    loadTokens();
  }

  void loadTokens() {
    refreshFetchDiagnostic();
    for (String scope : scopes) {
      try {
        fetchZign(scope).ifPresent(token -> scopeTokens.put(scope, token));
      } catch (Exception e) {
        logger.error("error setting token for scope={}, skipping", scope);
      }
    }
  }

  public void stop() {
    if (started.getAndSet(false)) {
      logger.info("stopping zign token background refresh, scopes {}", scopes);
      ExecutorServiceSupport.shutdown(executorService);
      logger.info("stopped zign token background refresh, scopes {}", scopes);
    }
  }

  private Optional<String> fetchZign(String scope) {

    String token = null;
    Process proc = null;
    try {
      proc = new ProcessBuilder()
          .command("zign", "token", scope)
          .redirectErrorStream(true)
          .start();

      boolean done = proc.waitFor(waitFor, TimeUnit.SECONDS);
      try (InputStream inputStream = proc.getInputStream()) {
        token = new Scanner(new InputStreamReader(inputStream, Charset.forName("UTF-8"))).next();
      }

      if (proc.exitValue() == 0) {
        logger.info("obtained token for scope={} process_exited={} exit_value={}",
            scope, done, proc.exitValue());
      } else {
        logger.error(
            "failed to obtain token for scope={} process_exited={} exit_value=[{}] scope={} response={}",
            scope, done, proc.exitValue(), token);
        token = null;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      logger.error("error fetching scope [{}] [{}]", scope, e.getMessage());
    } finally {
      if (proc != null && proc.isAlive()) {
        proc.destroyForcibly();
      }
    }

    return Optional.ofNullable(token);
  }

  private void refreshFetchDiagnostic() {
    if (refreshEvery < (waitFor * scopes.size())) {
      logger.warn(
          "refresh every time is less than total permissible fetch time for scopes refresh_every={} zign_wait=({} * {})"
          , refreshEvery, waitFor, scopes.size()
      );
    }
  }

  public static class Builder {
    Set<String> scopes = new HashSet<>();
    long refreshEvery = REFRESH_EVERY_SECONDS;
    long waitFor = WAIT_FOR_ZIGN_SECCONDS;

    public Builder() {
    }

    public Builder scopes(String... scopes) {
      NakadiException.throwNonNull(scopes, "Please provide some scopes");
      this.scopes.addAll(Arrays.asList(scopes));
      return this;
    }

    public Builder refreshEvery(long refreshEvery, TimeUnit unit) {
      NakadiException.throwNonNull(unit, "Please provide a time unit");
      this.refreshEvery = unit.toSeconds(refreshEvery);
      return this;
    }

    public Builder waitFor(long waitFor, TimeUnit unit) {
      NakadiException.throwNonNull(unit, "Please provide a time unit");
      this.waitFor = unit.toSeconds(waitFor);
      return this;
    }

    public TokenProviderZign build() {

      scopes(TokenProvider.NAKADI_CONFIG_WRITE,
          TokenProvider.NAKADI_EVENT_STREAM_READ,
          TokenProvider.NAKADI_EVENT_STREAM_WRITE,
          TokenProvider.NAKADI_EVENT_TYPE_WRITE,
          TokenProvider.UID);

      return new TokenProviderZign(this);
    }
  }
}
