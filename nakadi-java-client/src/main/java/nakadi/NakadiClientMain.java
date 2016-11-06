package nakadi;

import com.google.common.collect.Lists;
import com.google.gson.GsonBuilder;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import okhttp3.OkHttpClient;
import rx.Observable;

/**
 * Essentially a dummy main class to keep the Shadow plugin happy, but also lets us check the
 * shading step works ok.
 */
public class NakadiClientMain {

  public static void main(String[] args) throws Exception {
    final NakadiClientMain main = new NakadiClientMain();
    main.hello("thing one", "thing two");

    OkHttpClient client = new OkHttpClient();
    final List<InetAddress> localhost = client.dns().lookup("localhost");
    System.out.println("OkHttpClient found: " + localhost);
  }

  public static void hello(String... names) {

    final ArrayList<String> strings = Lists.newArrayList(names);
    new GsonBuilder().create().toJson(strings, System.out);
    Observable.from(names).subscribe(s -> System.out.println("Hello " + s + "!"));
  }
}
