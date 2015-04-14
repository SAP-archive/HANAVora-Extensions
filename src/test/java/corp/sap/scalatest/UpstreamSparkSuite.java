package corp.sap.scalatest;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks suites that test Apache Spark itself and not our extensions.
 */
@org.scalatest.TagAnnotation
@Retention(RUNTIME)
@Target({METHOD, TYPE})
public @interface UpstreamSparkSuite {
}
