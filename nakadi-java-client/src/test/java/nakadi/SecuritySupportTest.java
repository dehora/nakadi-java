package nakadi;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.OkHttpClient;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SecuritySupportTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void build() {
    OkHttpClient.Builder builder = new OkHttpClient.Builder();
    SecuritySupport securitySupport = new SecuritySupport("classpath:certs");

    try {
      securitySupport.applySslSocketFactory(builder);
      builder.build();
    } catch (Exception e) {
      fail("expected custom ssl/trust install to succeed " +e.getMessage());
    }
  }

  @Test
  public void createSome() throws IOException {
    /*
    certs contains the 2 letsencrypt root, with .crt and .pem file extensions
     */
    SecuritySupport securitySupport = new SecuritySupport("classpath:certs");

    X509TrustManager x509TrustManager = securitySupport.trustManager();
    SSLContext sslContext = securitySupport.sslContext();
    assertNotNull(x509TrustManager);
    assertNotNull(sslContext);

    X509Certificate[] acceptedIssuers = x509TrustManager.getAcceptedIssuers();

    assertEquals(2, acceptedIssuers.length);

    String issuer1 = "CN=Let's Encrypt Authority X1, O=Let's Encrypt, C=US";
    String issuer2 = "CN=Let's Encrypt Authority X2, O=Let's Encrypt, C=US";
    Set<String> seen = Sets.newHashSet();
    for (X509Certificate acceptedIssuer : acceptedIssuers) {
      String name = acceptedIssuer.getSubjectDN().getName();
      System.out.println(name);
      if (issuer1.equals(name)) {
        seen.add(name);
      }

      if (issuer2.equals(name)) {
        seen.add(name);
      }
    }

    assertEquals(2, seen.size());
    assertTrue(seen.contains(issuer1));
    assertTrue(seen.contains(issuer2));
  }

  @Test
  public void createNone() throws IOException {

    String fPath = "file://" + folder.newFolder().getAbsolutePath();
    SecuritySupport securitySupport = new SecuritySupport(fPath);

    X509TrustManager x509TrustManager = securitySupport.trustManager();
    SSLContext sslContext = securitySupport.sslContext();
    assertNotNull(x509TrustManager);
    assertNotNull(sslContext);

    X509Certificate[] acceptedIssuers = x509TrustManager.getAcceptedIssuers();
    assertEquals(0, acceptedIssuers.length);
  }

  @Test
  public void createNull() throws IOException {
    SecuritySupport securitySupport = new SecuritySupport(null);
    X509TrustManager x509TrustManager = securitySupport.trustManager();
    SSLContext sslContext = securitySupport.sslContext();
    assertNull(x509TrustManager);
    assertNull(sslContext);
  }

  @Test
  public void resolvePath() throws IOException {

    try {
      SecuritySupport.resolvePath("classpath:cer");
      fail("exception expected for unknown classpath");
    } catch (Exception ignored) {
    }

    try {
      SecuritySupport.resolvePath("file://woo");
      fail("exception expected for file uri component");
    } catch (Exception ignored) {
    }

    try {
      SecuritySupport.resolvePath("file:///woo");
      fail("exception expected for missing path");
    } catch (Exception ignored) {
    }

    Path pathC = SecuritySupport.resolvePath("classpath:certs");
    assertTrue(pathC != null);

    String fPath = "file://" + folder.newFolder().getAbsolutePath();
    Path pathF = SecuritySupport.resolvePath(fPath);
    assertTrue(pathF != null);
  }
}