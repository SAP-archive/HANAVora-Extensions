package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{AbstractView, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}

/**
  * Resolves view plans.
  */
case class ResolveViews(analyzer: Analyzer) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case a: AbstractView =>
      val withRelations = analyzer.ResolveRelations(a.plan)
      val withHierarchies = ResolveHierarchy(analyzer)(withRelations)
      ExcludeHierarchyNodeFromSelectStar(analyzer)(withHierarchies)
  }
}
