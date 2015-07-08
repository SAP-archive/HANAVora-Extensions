package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Marks a data source as supporting expressions.
 */
trait ExpressionSupport {

  def supports(expr: Expression): Boolean

}
