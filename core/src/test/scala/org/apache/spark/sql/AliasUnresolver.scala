package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, Expression}

/**
 * Calling this object on an expression (or expressions) puts a [[UnresolvedAlias]] on each one
 * of its children.
 */
object AliasUnresolver {
  def apply(children: Expression*): Seq[NamedExpression] = {
    children.map({
      case child => UnresolvedAlias(child)
    })
  }
}
