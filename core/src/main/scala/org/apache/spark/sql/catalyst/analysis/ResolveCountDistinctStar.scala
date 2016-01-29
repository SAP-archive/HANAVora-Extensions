package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
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
          case u@UnresolvedAlias(
            aggExp@AggregateExpression(c@Count((star: UnresolvedStar) :: Nil),_ , true)) =>
              val expanded = star.expand(child, analyzer.resolver)
              u.copy(aggExp.copy(c.copy(expanded)))
          case default => default
        })
      )
  }
}
