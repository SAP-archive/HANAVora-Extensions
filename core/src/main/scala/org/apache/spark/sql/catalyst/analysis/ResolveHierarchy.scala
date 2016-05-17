package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical._
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
    case h:Hierarchy if !h.node.resolved => resolveHierarchyNodeAttr(h)
    /**
      * Once the child relation is resolved, we can resolve the parenthood expression.
      */
    case h@ Hierarchy(spec: HierarchySpec, _) if !h.resolved && spec.source.resolved =>
      resolveSpec(h)
  }

  /** Resolve the node attribute by creating an [[AttributeReference]] with its name. */
  private[this] def resolveHierarchyNodeAttr(h: Hierarchy): Hierarchy = {
    val a = h.node
    h.copy(node = AttributeReference(a.name, NodeType, nullable = false)())
  }

  private[this] def resolveSpec(h: Hierarchy): Hierarchy = h match {
    case h @ Hierarchy(spec: LevelBasedHierarchySpec, _) =>
      h.copy(spec = spec.copy(levels = spec.levels.map {
        case u@UnresolvedAttribute(nameParts) =>
          spec.resolveSpec(nameParts, analyzer.resolver).getOrElse(u)
        case other => other
      }))
    case h @ Hierarchy(spec: AdjacencyListHierarchySpec, _) =>
      h.copy(spec = spec.copy(parenthoodExp = spec.parenthoodExp.mapChildren {
        case u@UnresolvedAttribute(nameParts) =>
          spec.resolveSpec(nameParts, analyzer.resolver).getOrElse(u)
        case other => other
      }))
  }
}
