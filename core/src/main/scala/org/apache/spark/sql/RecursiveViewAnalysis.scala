package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{AbstractView, LogicalPlan}
import org.apache.spark.sql.execution.datasources.AbstractCreateViewCommand

/**
 * Checks that views cannot be recursively defined on themselves.
 */
object RecursiveViewAnalysis {
  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case c:AbstractCreateViewCommand[_] if containsViewIdentifier(c.identifier, c.view.plan) =>
        throw new AnalysisException(s"The view ${c.identifier.table} " +
          s"cannot be defined recursively.")
      case _ =>
    }
  }

  private def containsViewIdentifier(name: TableIdentifier,
                                     plan: LogicalPlan): Boolean = plan.find {
    case UnresolvedRelation(ident, _) if ident == name =>
      true
    case AbstractView(child) => containsViewIdentifier(name, child)
    case _ =>
      false
  }.isDefined
}
