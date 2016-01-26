package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This is a simple analysis rule that removes all [[AnnotatedAttribute]] from the plan.
 * @param analyzer The analyzer.
 */
private[sql]
case class AnnotationResolver(analyzer: Analyzer) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val result = plan transformUp {
      case Project(projectList, child) =>
        val resolvedProjectionList = projectList.map({
          case AnnotatedAttribute(x:NamedExpression) => x
          case default => default
        })
        Project(resolvedProjectionList, child)
      case default => default
    }
    result
  }

}
