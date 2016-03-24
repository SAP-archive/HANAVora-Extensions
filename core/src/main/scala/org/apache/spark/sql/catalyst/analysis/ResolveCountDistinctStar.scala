package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.CountDistinct
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Resolves COUNT(DISTINCT *) queries
 * @param analyzer The analyzer
 */
case class ResolveCountDistinctStar(analyzer: Analyzer) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case a@Aggregate(_, aggregateExpressions, child) =>
      analyzer.ResolveAliases(
        a.copy(aggregateExpressions = aggregateExpressions.collect {
          case u@UnresolvedAlias(c@CountDistinct((star: UnresolvedStar) :: Nil)) =>
            val expanded = star.expand(child.output, analyzer.resolver)
            u.copy(c.copy(expanded))
          case default => default
        })
      )
  }
}
