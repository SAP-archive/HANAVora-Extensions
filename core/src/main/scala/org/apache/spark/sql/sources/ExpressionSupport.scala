package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Marks a data source as supporting expressions.
 */
trait ExpressionSupport {

  /**
   * Checks whether a expression is supported or not by the data source.
   *
   * @param expr Expression to be checked
   * @return true if supported, false otherwise.
   */
  def supports(expr: Expression): Boolean

}
