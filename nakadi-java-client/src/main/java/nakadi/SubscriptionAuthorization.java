package nakadi;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SubscriptionAuthorization {

    private List<AuthorizationAttribute> admins;
    private List<AuthorizationAttribute> readers;

    /**
     * @return an immutable copy of the underlying admin authorization list
     */
    public List<AuthorizationAttribute> admins() {
        return ImmutableList.copyOf(admins);
    }

    public SubscriptionAuthorization admins(List<AuthorizationAttribute> admins) {
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

    public SubscriptionAuthorization readers(List<AuthorizationAttribute> readers) {
        NakadiException.throwNonNull(readers, "Please provide a non-null readers authorization list");

        guardReaders();
        this.readers.addAll(readers);
        return this;
    }

    public SubscriptionAuthorization admin(AuthorizationAttribute admin) {
        NakadiException.throwNonNull(admin, "Please provide a non-null admin authorization");
        guardAdmins();
        this.admins.add(admin);
        return this;
    }

    public SubscriptionAuthorization reader(AuthorizationAttribute reader) {
        NakadiException.throwNonNull(reader, "Please provide a non-null reader authorization");
        guardReaders();
        this.readers.add(reader);
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

    @Override public String toString() {
        return "EventTypeAuthorization{" + "admins=" + admins +
                ", readers=" + readers +
                '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubscriptionAuthorization that = (SubscriptionAuthorization) o;
        return Objects.equals(admins, that.admins) &&
                Objects.equals(readers, that.readers);
    }

    @Override public int hashCode() {
        return Objects.hash(admins, readers);
    }
}
