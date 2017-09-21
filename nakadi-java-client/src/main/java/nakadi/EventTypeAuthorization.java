package nakadi;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Authorization section for an event type.
 * <p>
 *   This  defines three access control lists: one for producing events
 *   ('writers'), one for consuming events ('readers'), and one for
 *   administering an event type ('admins'). Regardless of the values
 *   of the authorization properties, administrator accounts will always
 *   be authorized.
 * </p>
 */
public class EventTypeAuthorization {

  private List<AuthorizationAttribute> admins;
  private List<AuthorizationAttribute> readers;
  private List<AuthorizationAttribute> writers;

  /**
   * @return an immutable copy of the underlying admin authorization list
   */
  public List<AuthorizationAttribute> admins() {
    return ImmutableList.copyOf(admins);
  }

  public EventTypeAuthorization admins(List<AuthorizationAttribute> admins) {
    NakadiException.throwNonNull(admins, "Please provide a non-null admin authorization list");

    guardAdmins();
    this.admins.addAll(admins);
    return this;
  }

  /**
   * @return an immutable copy of the underlying reader authorization list
   */
  public List<AuthorizationAttribute> readers() {
    return ImmutableList.copyOf(readers);
  }

  public EventTypeAuthorization readers(List<AuthorizationAttribute> readers) {
    NakadiException.throwNonNull(readers, "Please provide a non-null readers authorization list");

    guardReaders();
    this.readers.addAll(readers);
    return this;
  }

  /**
   * @return an immutable copy of the underlying writer authorization list
   */
  public List<AuthorizationAttribute> writers() {
    return ImmutableList.copyOf(writers);
  }

  public EventTypeAuthorization writers(List<AuthorizationAttribute> writers) {
    NakadiException.throwNonNull(writers, "Please provide a non-null writers authorization list");
    guardWriters();
    this.writers.addAll(writers);
    return this;
  }

  public EventTypeAuthorization admin(AuthorizationAttribute admin) {
    NakadiException.throwNonNull(admin, "Please provide a non-null admin authorization");
    guardAdmins();
    this.admins.add(admin);
    return this;
  }

  public EventTypeAuthorization reader(AuthorizationAttribute reader) {
    NakadiException.throwNonNull(reader, "Please provide a non-null reader authorization");
    guardReaders();
    this.readers.add(reader);
    return this;
  }

  public EventTypeAuthorization writer(AuthorizationAttribute writer) {
    NakadiException.throwNonNull(writer, "Please provide a non-null writer authorization");
    guardWriters();
    this.writers.add(writer);
    return this;
  }


  private void guardAdmins() {
    if (this.admins == null) {
      this.admins = new ArrayList<>();
    }
  }

  private void guardReaders() {
    if (this.readers == null) {
      this.readers = new ArrayList<>();
    }
  }

  private void guardWriters() {
    if (this.writers == null) {
      this.writers = new ArrayList<>();
    }
  }

  @Override public String toString() {
    return "EventTypeAuthorization{" + "admins=" + admins +
        ", readers=" + readers +
        ", writers=" + writers +
        '}';
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventTypeAuthorization that = (EventTypeAuthorization) o;
    return Objects.equals(admins, that.admins) &&
        Objects.equals(readers, that.readers) &&
        Objects.equals(writers, that.writers);
  }

  @Override public int hashCode() {
    return Objects.hash(admins, readers, writers);
  }
}
