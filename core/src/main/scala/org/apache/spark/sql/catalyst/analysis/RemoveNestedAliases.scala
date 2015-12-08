package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * Drop nested aliases, since they are useless in Spark SQL and invalid in SQL.
  *
  * TODO: This might be moved to [[org.apache.spark.sql.sources.sql]].
  */
object RemoveNestedAliases extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case p => p transformExpressionsUp {
        /** Drop alias inside aliases */
        case a@Alias(Alias(child, _), name) =>
          Alias(child, name)(
            exprId = a.exprId,
            qualifiers = a.qualifiers,
            explicitMetadata = a.explicitMetadata
          )
        /** Drop alias inside expressions */
        case exp if !exp.isInstanceOf[Alias] =>
          exp.transformUp { case Alias(child, _) => child }
      }
    }
}
