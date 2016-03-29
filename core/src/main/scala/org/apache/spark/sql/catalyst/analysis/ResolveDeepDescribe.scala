package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.DeepDescribeCommand
import org.apache.spark.sql.sources.commands.UnresolvedDeepDescribe
import org.apache.spark.sql.sources.describable.Describable

case class ResolveDeepDescribe(analyzer: Analyzer)
  extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case UnresolvedDeepDescribe(relation) =>
      DeepDescribeCommand(Describable(analyzer.execute(relation)))
    case default => default
  }
}
