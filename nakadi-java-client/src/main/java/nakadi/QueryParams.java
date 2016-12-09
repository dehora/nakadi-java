package nakadi;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class QueryParams {

  private final ListMultimap<String, String> params = ArrayListMultimap.create();
  private final Set<String> paramsToReplace = new HashSet<>();

  public QueryParams param(String key, String value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    params.put(key, value);
    return this;
  }

  public QueryParams param(String key, String... values) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(values);
    params.putAll(key, Arrays.asList(values));
    return this;
  }

  QueryParams paramReplacing(String key, String value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    ArrayList<String> values = new ArrayList<>(1);
    Collections.addAll(values, value);
    params.replaceValues(key, values);
    paramsToReplace.add(key);
    return this;
  }

  QueryParams paramReplacing(String key, String... values) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(values);
    params.replaceValues(key, Arrays.asList(values));
    paramsToReplace.add(key);
    return this;
  }

  public List<String> param(String key) {
    return params.get(key);
  }

  public Map<String, Collection<String>> params() {
    return params.asMap();
  }

  Multimap<String, String> multimap() {
    return params;
  }

  Set<String> paramsToReplace() {
    return paramsToReplace;
  }
}
