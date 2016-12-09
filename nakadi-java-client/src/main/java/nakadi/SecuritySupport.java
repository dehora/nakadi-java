package nakadi;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class SecuritySupport {

  private static final Logger logger = LoggerFactory.getLogger(NakadiClient.class.getSimpleName());

  private static URL getResourceUrl(String resourceName) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    classLoader = classLoader == null ? SecuritySupport.class.getClassLoader() : classLoader;
    URL url = classLoader.getResource(resourceName);
    NakadiException.throwNonNull(url, "resource not found: "+ resourceName);
    return url;
  }

  private final String certificatePath;
  private SSLContext sslContext;
  private X509TrustManager trustManager;

  public SecuritySupport(String certificatePath) {
    try {
      MDC.put("security_context", "[security_support]");
      this.certificatePath = certificatePath;
      create(this.certificatePath);
    } finally {
      MDC.remove("security_context");
    }
  }

  void applySslSocketFactory(OkHttpClient.Builder builder)
      throws NakadiException {

    try {
      MDC.put("security_context", "[security_support]");
      if (certificatePath == null) {
        logger.info("no custom certificate path supplied, using system default");
        return;
      }

      builder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
      logger.info("ok, custom socket factory and trust manager added to builder");
    } finally {
      MDC.remove("security_context");
    }
  }

  void create(String certificatePath)
      throws NakadiException {

      if (certificatePath == null) {
        logger.info("no custom certificate path supplied, skipping creation");
        return;
      }

      Path path = SecuritySupport.resolvePath(certificatePath);

      try {
        logger.info("will create custom certs from path {}", path.toRealPath());
        create(path);
      } catch (Exception e) {
        throw new NakadiException(
            Problem.localProblem("configuring keystore failed, path " + certificatePath,
                e.getMessage()),
            e);
      }
    }


  private void create(Path path)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
      KeyManagementException {
    TrustManager[] trustManagers;
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(null, null);

    installCertificates(path, keyStore);

    String defaultAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(defaultAlgorithm);
    trustManagerFactory.init(keyStore);
    trustManagers = trustManagerFactory.getTrustManagers();
    sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, trustManagers, null);
    trustManager = (X509TrustManager) trustManagers[0];
    X509Certificate[] acceptedIssuers = trustManager.getAcceptedIssuers();
    for (X509Certificate acceptedIssuer : acceptedIssuers) {
      logger.info("installed cert details: subject={} issuer={}",
          acceptedIssuer.getSubjectX500Principal(), acceptedIssuer.getIssuerX500Principal());
    }
  }

  private void installCertificates(Path path, KeyStore keyStore)
      throws IOException, CertificateException {
    CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");

    try (DirectoryStream<Path> paths = Files.newDirectoryStream(path, "*.{crt,pem}")) {
      for (Path certPath : paths) {
        logger.info("installing cert from path {}", certPath.toRealPath());
        if (Files.isRegularFile(certPath)) {
          try (InputStream inputStream = Files.newInputStream(certPath)) {
            Certificate cert = certificateFactory.generateCertificate(inputStream);
            String alias = certPath.getFileName().toString();
            keyStore.setCertificateEntry(alias, cert);
            logger.info("ok, installed cert with alias {} from path {}", alias,
                certPath.toRealPath());
          } catch (Exception e) {
            logger.warn("error, skipping cert, path {} {}", certPath.toRealPath(), e.getMessage());
          }
        } else {
          logger.info("skipping cert, not a regular file {}", certPath.toRealPath());
        }
      }
    }
  }

  @VisibleForTesting
  static Path resolvePath(String certificatePath){
    Path path;
    if (certificatePath.startsWith("file:")) {
      try {
        logger.info("using file resolver for {}", certificatePath);
        path = Paths.get(new URL(certificatePath).toURI());
        if (!Files.exists(path)) {
          throw new FileNotFoundException();
        }
        return path;
      } catch (Exception e) {
        throw new NakadiException(
            Problem.localProblem("certificatePath resolver failed " + certificatePath,
                e.getMessage()), e);
      }
    }

    if (certificatePath.startsWith("classpath:")) {
      logger.info("using classpath resolver for {}", certificatePath);
      String resource = getResourceUrl(
          certificatePath.substring("classpath:".length(), certificatePath.length())).getPath();
      return Paths.get(resource);
    }

    throw new NakadiException(
        Problem.localProblem(
            "certificatePath must start with file: or classpath: " + certificatePath, ""));
  }

  public SSLContext sslContext() {
    return sslContext;
  }

  public X509TrustManager trustManager() {
    return trustManager;
  }


}
