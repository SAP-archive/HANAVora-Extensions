package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{Hierarchy, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Resolves [[Hierarchy]]s.
 */
case class ResolveHierarchy(analyzer : Analyzer) extends Rule[LogicalPlan] {

  // scalastyle:off cyclomatic.complexity

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case h : Hierarchy if h.relation.resolved &&
      h.parenthoodExpression.resolved && !h.nodeAttribute.resolved =>
      resolveNodeAttribute(h)
    case h : Hierarchy if !h.resolved && h.relation.resolved && !h.parenthoodExpression.resolved =>
      resolveParenthoodExpression(h)
    case p: LogicalPlan if !p.childrenResolved => p
  }

  // scalastyle:on cyclomatic.complexity

  private def resolveNodeAttribute(h : Hierarchy) : Hierarchy =
    h.copy(nodeAttribute = h.resolveNodeAttribute().getOrElse(h.nodeAttribute))

  private def resolveParenthoodExpression(h : Hierarchy) : Hierarchy =
    h.copy(parenthoodExpression = h.parenthoodExpression.mapChildren({
      case u @ UnresolvedAttribute(name) =>
        h.resolveParenthoodExpression(name, analyzer.resolver).getOrElse(u)
      case other => other
    }))

}
