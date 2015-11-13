package org.apache.spark.sql.catalyst.expressions.compat;

//
// Backported from Spark 1.5.
//

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.apache.spark.annotation.DeveloperApi;

/**
 * ::DeveloperApi::
 * A function description type which can be recognized by FunctionRegistry, and will be used to
 * show the usage of the function in human language.
 *
 * `usage()` will be used for the function usage in brief way.
 * `extended()` will be used for the function usage in verbose way, suppose
 *              an example will be provided.
 *
 *  And we can refer the function name by `_FUNC_`, in `usage` and `extended`, as it's
 *  registered in `FunctionRegistry`.
 */
@DeveloperApi
@Retention(RetentionPolicy.RUNTIME)
public @interface ExpressionDescription {
  String usage() default "_FUNC_ is undocumented";
  String extended() default "No example for _FUNC_.";
}
