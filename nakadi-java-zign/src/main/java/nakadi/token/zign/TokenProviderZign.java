package nakadi.token.zign;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.Scanner;
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
  private static final int WAIT_FOR_ZIGN_SECCONDS = 6;
  private static final String THREAD_NAME = "nakadi-java-zign";

  private long refreshEvery = REFRESH_EVERY_SECONDS;
  private long waitFor = WAIT_FOR_ZIGN_SECCONDS;
  // not provided by builder
  private final AtomicBoolean started = new AtomicBoolean(false);
  private ScheduledExecutorService executorService;
  private String token = null;

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override public Optional<String> authHeaderValue(@Deprecated String scope) {
    if (scope != null) {
      logger.warn("Scopes are deprecated in Nakadi and should not be supplied as an option.");
    }

    return Optional.of("Bearer " + token);
  }

  private TokenProviderZign(Builder builder) {
    this.refreshEvery = builder.refreshEvery;
    this.waitFor = builder.waitFor;
  }

  public void start() {

    if (!started.getAndSet(true)) {
      loadToken(); // first time run, then refresh in the background
      executorService = Executors.newSingleThreadScheduledExecutor();
      executorService.scheduleAtFixedRate(this::refreshToken, refreshEvery, refreshEvery,
          TimeUnit.SECONDS);

      logger.info("refreshing tokens in background every {} seconds", refreshEvery);
    }
  }

  void refreshToken() {
    Thread.currentThread().setName(THREAD_NAME);
    logger.info("refreshing token");
    loadToken();
  }

  void loadToken() {
    refreshFetchDiagnostic();
    fetchZign().ifPresent(token -> this.token = token);
  }

  public void stop() {
    if (started.getAndSet(false)) {
      logger.info("stopping zign token background refresh {}");
      ExecutorServiceSupport.shutdown(executorService);
      logger.info("stopped zign token background refresh {}");
    }
  }

  private Optional<String> fetchZign() {

    String token = null;
    Process proc = null;
    try {
      proc = new ProcessBuilder()
          .command("zign", "token")
          .redirectErrorStream(true)
          .start();

      boolean done = proc.waitFor(waitFor, TimeUnit.SECONDS);
      try (InputStream inputStream = proc.getInputStream()) {
        token = new Scanner(new InputStreamReader(inputStream, Charset.forName("UTF-8"))).next();
      }

      if (proc.exitValue() == 0) {
        logger.info("obtained token process_exited={} exit_value={}",
            done, proc.exitValue());
      } else {
        logger.error(
            "failed to obtain token process_exited={} exit_value=[{}] scope={} response={}",
            done, proc.exitValue(), token);
        token = null;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      logger.error("error fetching token [{}] [{}]",e.getMessage());
    } finally {
      if (proc != null && proc.isAlive()) {
        proc.destroyForcibly();
      }
    }

    return Optional.ofNullable(token);
  }

  private void refreshFetchDiagnostic() {
    if (refreshEvery < waitFor) {
      logger.warn(
          "refresh every time is less than total permissible fetch time refresh_every={} zign_wait={}"
          , refreshEvery, waitFor
      );
    }
  }

  public static class Builder {
    long refreshEvery = REFRESH_EVERY_SECONDS;
    long waitFor = WAIT_FOR_ZIGN_SECCONDS;

    public Builder() {
    }

    @Deprecated
    public Builder scopes(String... scopes) {
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
      return new TokenProviderZign(this);
    }
  }
}
