package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{AppendRunnableCommand, LogicalRelation}
import org.apache.spark.sql.sources.AppendRelation
import org.apache.spark.sql.sources.commands.UnresolvedAppendCommand

case class ResolveAppendCommand(analyzer: Analyzer) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case UnresolvedAppendCommand(table, options) =>
      val resolvedTable = analyzer.execute(table)
      resolvedTable.collectFirst {
        case LogicalRelation(appendRelation: AppendRelation, _) =>
          AppendRunnableCommand(appendRelation, options)
      }.getOrElse {
          throw new AnalysisException(s"Cannot append ${resolvedTable.treeString}")
      }
  }
}
