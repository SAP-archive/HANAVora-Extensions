package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Rule used to drop Alias inside Aliases and also to drop Aliases inside expressions
 */
object DropUnneededAliases extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case p => p transformExpressionsUp {
        /* Drop alias inside aliases */
        case a@Alias(Alias(child, _), name) =>
          Alias(child, name)(
            exprId = a.exprId,
            qualifiers = a.qualifiers,
            explicitMetadata = a.explicitMetadata
          )
        /* Drop alias inside expressions */
        case exp: Expression if !exp.isInstanceOf[Alias] => exp.transformUp {
          case a@Alias(child, _) => child
        }
      }
    }
}
