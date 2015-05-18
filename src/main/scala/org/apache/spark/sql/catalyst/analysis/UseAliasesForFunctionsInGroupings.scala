package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Converts any aggregation expression present in the grouping expressions list
 * (i.e. GROUP BY) to use aliases. This is not intended to be used with the standard
 * analysis or optimization phases, but some [[org.apache.spark.sql.sources.CatalystSource]]
 * implementations use it.
 */
object UseAliasesForFunctionsInGroupings extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp {
      case agg@Aggregate(groupingExpressions, aggregateExpressions, child) =>
        val fixedGroupingExpressions = groupingExpressions.map({
          case e: AttributeReference => e
          case e =>
            val aliasOpt = aggregateExpressions.find({
              case Alias(aliasChild, aliasName) => aliasChild == e
              case _ => false
            })
            aliasOpt match {
              case Some(alias) => alias.toAttribute
              case None => sys.error(s"Cannot resolve Alias for $e")
            }
        })
        agg.copy(groupingExpressions = fixedGroupingExpressions)
    }

}
