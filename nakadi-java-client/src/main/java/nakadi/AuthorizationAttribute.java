package nakadi;

import java.util.Objects;

/**
 * An attribute for authorization. This object includes a data type, which
 * represents the type of the attribute and a value. A wildcard can be
 * represented with data type '*', and value '*' which means that all authenticated
 * users are allowed to perform an operation.
 */
public class AuthorizationAttribute {

  public static final String WILDCARD = "*";

  private String dataType;
  private String value;

  public String dataType() {
    return dataType;
  }

  public AuthorizationAttribute dataType(String dataType) {
    NakadiException.throwNonNull(dataType, "Please provide a non-null data type field.");
    this.dataType = dataType;
    return this;
  }

  public String value() {
    return value;
  }

  public AuthorizationAttribute value(String value) {
    NakadiException.throwNonNull(value, "Please provide a non-null value field.");
    this.value = value;
    return this;
  }

  @Override public String toString() {
    return "AuthorizationAttribute{" + "dataType='" + dataType + '\'' +
        ", value='" + value + '\'' +
        '}';
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AuthorizationAttribute that = (AuthorizationAttribute) o;
    return Objects.equals(dataType, that.dataType) &&
        Objects.equals(value, that.value);
  }

  @Override public int hashCode() {
    return Objects.hash(dataType, value);
  }
}
