package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.sources._

private[sql] trait VelocityCommandsSQLContextExtension
  extends DDLParserSQLContextExtension
  with PlannerSQLContextExtension {

  override protected def extendedDdlParser(parser: String => LogicalPlan): DDLParser =
    new VelocityDDLParser(parser)

  override def strategies(planner: ExtendedPlanner): List[Strategy] =
    VelocityDDLStrategy(planner) :: super.strategies(planner)

  private[sql] case class VelocityDDLStrategy(planner : ExtendedPlanner) extends Strategy {

    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan.flatMap({
      case AppendCommand(table, options) =>
        val logicalRelation = planner.optimizedPlan(table).asInstanceOf[LogicalRelation]
        val appendRelation = logicalRelation.relation.asInstanceOf[AppendRelation]
        ExecutedCommand(AppendRunnableCommand(appendRelation, options)) :: Nil
      case DropCommand(table) =>
        val logicalRelation = planner.optimizedPlan(table).asInstanceOf[LogicalRelation]
        val dropRelation = logicalRelation.relation.asInstanceOf[DropRelation]
        ExecutedCommand(DropRunnableCommand(dropRelation)) :: Nil
      case ShowDatasourceTablesCommand(provider, options) =>
        ExecutedCommand(ShowDataSourceTablesRunnableCommand(provider, options)) :: Nil
      case RegisterAllTablesUsing(provider, options, ignoreConflicts) =>
        ExecutedCommand(RegisterAllTablesCommand(provider, options, ignoreConflicts)) :: Nil
      case cv@CreateViewCommand(name, query) =>
        ExecutedCommand(cv) :: Nil
      case _ => Nil
    }).headOption.toSeq
  }
}
