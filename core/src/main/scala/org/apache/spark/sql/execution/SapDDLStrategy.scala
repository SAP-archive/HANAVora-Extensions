package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.extension.ExtendedPlanner
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.commands._

private[sql] case class SapDDLStrategy(planner: ExtendedPlanner) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan.flatMap({
    case AppendCommand(table, options) =>
      val logicalRelation = planner.optimizedPlan(table).asInstanceOf[LogicalRelation]
      val appendRelation = logicalRelation.relation.asInstanceOf[AppendRelation]
      ExecutedCommand(AppendRunnableCommand(appendRelation, options)) :: Nil
    case DropCommand(allowNotExisting, table, cascade) =>
      val dropRelation = planner.optimizedRelationLookup(table)
                                .map(_.asInstanceOf[LogicalRelation]
                                      .relation.asInstanceOf[DropRelation])
      ExecutedCommand(DropRunnableCommand(allowNotExisting, dropRelation, cascade)) :: Nil
    case ShowDatasourceTablesCommand(provider, options) =>
      ExecutedCommand(ShowDataSourceTablesRunnableCommand(provider, options)) :: Nil
    case RegisterAllTablesUsing(provider, options, ignoreConflicts) =>
      ExecutedCommand(RegisterAllTablesCommand(provider, options, ignoreConflicts)) :: Nil
    case RegisterTableUsing(tableName, provider, options, ignoreConflicts) =>
      ExecutedCommand(RegisterTableCommand(tableName, provider, options, ignoreConflicts)) :: Nil
    case cv@CreateViewCommand(name, query) =>
      ExecutedCommand(cv) :: Nil
    case cmd@UseStatementCommand(input) =>
      ExecutedCommand(cmd) :: Nil
    case _ => Nil
  }).headOption.toSeq

}
