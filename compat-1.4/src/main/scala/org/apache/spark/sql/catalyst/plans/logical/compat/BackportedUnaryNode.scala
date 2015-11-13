package org.apache.spark.sql.catalyst.plans.logical.compat

import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode

private[sql] trait BackportedUnaryNode extends UnaryNode {
  self: Product =>

  protected def resolve(
                         nameParts: Seq[String],
                         input: Seq[Attribute],
                         resolver: analysis.Resolver): Option[NamedExpression] =
    resolve(nameParts, input, resolver, throwErrors = false)
}
