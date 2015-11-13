package org.apache.spark.sql.catalyst.analysis.compat

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.Expression

object BackportedUnresolvedFunction {

  def unapply(any: Any): Option[(String, Seq[Expression], Boolean)] =
    any match {
      case UnresolvedFunction(name, children) =>
        Some((name, children, false))
      case _ =>
        None
    }

}
