package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.NodeType

/**
  * Excludes the [[org.apache.spark.sql.types.NodeType]] column from the top-level star.
  * @param analyzer The analyzer
  */
case class ExcludeHierarchyNodeFromSelectStar(analyzer: Analyzer) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan match {
      case p@Project((s@UnresolvedStar(None)) :: Nil, child) if child.output.nonEmpty =>
        p.copy(s.expand(child, analyzer.resolver).filterNot(p => p.dataType == NodeType))
      case p@Project((s@UnresolvedStar(Some(t))) :: Nil, child) if t.size == 1 =>
        p.copy(s.expand(child, analyzer.resolver).filterNot(p => p.dataType == NodeType))
      case default => default
    }
}
