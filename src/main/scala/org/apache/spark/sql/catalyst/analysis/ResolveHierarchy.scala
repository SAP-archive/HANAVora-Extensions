package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{Hierarchy, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.NodeType

/**
 * Resolves [[Hierarchy]]s.
 */
case class ResolveHierarchy(analyzer : Analyzer) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case h : Hierarchy if !h.nodeAttribute.resolved =>
      val a = h.nodeAttribute
      h.copy(nodeAttribute = AttributeReference(a.name, NodeType, nullable = false)())
    case h : Hierarchy if !h.resolved && h.relation.resolved && !h.parenthoodExpression.resolved =>
      resolveParenthoodExpression(h)
    case p: LogicalPlan => p
  }

  private def resolveParenthoodExpression(h : Hierarchy) : Hierarchy =
    h.copy(parenthoodExpression = h.parenthoodExpression.mapChildren({
      case u @ UnresolvedAttribute(nameParts) =>
        h.resolveParenthoodExpression(nameParts, analyzer.resolver).getOrElse(u)
      case other => other
    }))

}
