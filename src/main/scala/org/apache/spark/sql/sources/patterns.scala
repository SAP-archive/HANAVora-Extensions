package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical

object SelectOperation extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], logical.LogicalPlan)

  def unapply(plan: logical.LogicalPlan): Option[ReturnType] =
    plan match {
      case _: logical.Project | _: logical.Filter => PhysicalOperation.unapply(plan)
      case _ => None
    }

}


object SetOperation extends PredicateHelper
{
  type ReturnType = (String,logical.LogicalPlan,logical.LogicalPlan)
  def unapply(plan:logical.LogicalPlan):Option[ReturnType]=
  plan match {
    case logical.Distinct(child:logical.Union) => Some("UNION",child.left,child.right)
    case logical.Union(left,right) => Some("UNION ALL",left,right)
    case logical.Intersect(left,right) => Some("INTERSECT",left,right)
    case logical.Except(left,right) => Some("EXCEPT",left,right)
    case _ => None
  }
}


object GroupByOperation extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], Seq[Expression], logical.LogicalPlan)

  def unapply(plan: logical.LogicalPlan): Option[ReturnType] =
    plan match {
      case logical.Aggregate(groupingExpressions, aggregateExpressions, child) =>
        val (_, filters, newChild, aliases) = PhysicalOperation.collectProjectsAndFilters(child)
        val substitutedAggregateExpressions = aggregateExpressions
          .map(PhysicalOperation.substitute(aliases))
          .asInstanceOf[Seq[NamedExpression]]
        val substitutedGroupingExpressions = groupingExpressions
          .map(PhysicalOperation.substitute(aliases))
        Some((substitutedAggregateExpressions, filters, substitutedGroupingExpressions, newChild))
      case _ => None
    }
}



