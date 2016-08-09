package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{Hierarchy, LogicalPlan, Project, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.NodeType

import scala.annotation.tailrec

/**
  * Excludes the [[org.apache.spark.sql.types.NodeType]] column from the top-level star.
  * @param analyzer The analyzer
  */
case class ExcludeHierarchyNodeFromSelectStar(analyzer: Analyzer) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp {
      case project@Project(projectList, child) if leadsToHierarchy(child) =>
        /** Expands the given [[UnresolvedStar]] and removes attributes with [[NodeType]] */
        def expand(star: UnresolvedStar) =
          star
            .expand(child, analyzer.resolver)
            .filterNot(expr => expr.resolved && expr.dataType == NodeType)

        val filtered = projectList.flatMap {
          case UnresolvedAlias(u: UnresolvedStar) => expand(u)
          case u: UnresolvedStar => expand(u)
          case default => Seq(default)
        }
        project.copy(filtered)
    }


  /**
    * Checks whether the given plan leads to a [[Hierarchy]].
    *
    * The given path may contain [[Subquery]]s, since they do not alter the output.
    * @param plan The [[LogicalPlan]] to check for a path to a [[Hierarchy]].
    * @return `true` if the plan has a path to a [[Hierarchy]], `false` otherwise.
    */
  @tailrec
  private def leadsToHierarchy(plan: LogicalPlan): Boolean = plan match {
    case h: Hierarchy => true
    case Subquery(_, child) => leadsToHierarchy(child)
    case _ => false
  }
}
