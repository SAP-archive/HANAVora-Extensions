package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{AbstractView, LogicalPlan, View}
import org.apache.spark.sql.execution.datasources.CreateViewCommand

/**
 * Checks that views cannot be recursively defined on themselves.
 */
object RecursiveViewAnalysis {
  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case CreateViewCommand(name, _, query) if containsViewIdentifier(name, query) =>
        throw new AnalysisException(s"The view $name cannot be defined recursively.")
      case _ =>
    }
  }

  private def containsViewIdentifier(name: String, plan: LogicalPlan): Boolean = plan.find {
    case UnresolvedRelation(ident, _) if Seq(name) == ident =>
      true
    case AbstractView(child) => containsViewIdentifier(name, child)
    case _ =>
      false
  }.isDefined
}
