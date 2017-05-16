package nakadi;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotates that a method or class is experimental. Experimental API's might change or be
 * removed in later releases. Often they are present to support candidate features in the
 * Nakadi API.
 */
@Retention(value = RUNTIME)
@Target(value = {TYPE, METHOD, PARAMETER, CONSTRUCTOR})
public @interface Experimental {

}
