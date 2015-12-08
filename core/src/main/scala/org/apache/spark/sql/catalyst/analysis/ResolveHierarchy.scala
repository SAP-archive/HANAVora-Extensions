package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{Hierarchy, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.NodeType

/**
  * Resolves [[Hierarchy]]s.
  *
  * The particularities of resolving a [[Hierarchy]] node are:
  *   - The node attribute.
  *   - The parenthood expression.
  */
private[sql]
case class ResolveHierarchy(analyzer: Analyzer) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {

    /**
      * First thing, we resolve the node attribute. This will always happen in the first
      * analysis iteration, since it does not rely on resolving any other part of the plan.
      */
    case h: Hierarchy if !h.nodeAttribute.resolved =>
      resolveNodeAttribute(h)

    /**
      * Once the child relation is resolved, we can resolve the parenthood expression.
      */
    case h: Hierarchy if !h.resolved && h.relation.resolved && !h.parenthoodExpression.resolved =>
      resolveParenthoodExpression(h)

  }

  /** Resolve the node attribute by creating an [[AttributeReference]] with its name. */
  private[this] def resolveNodeAttribute(h: Hierarchy): Hierarchy = {
    val a = h.nodeAttribute
    h.copy(nodeAttribute = AttributeReference(a.name, NodeType, nullable = false)())
  }

  /** Resolve the parenthood expression. */
  private[this] def resolveParenthoodExpression(h: Hierarchy): Hierarchy =
    h.copy(parenthoodExpression = h.parenthoodExpression.mapChildren({
      case u @ UnresolvedAttribute(nameParts) =>
        h.resolveParenthoodExpression(nameParts, analyzer.resolver).getOrElse(u)
      case other => other
    }))

}
