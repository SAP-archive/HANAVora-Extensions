package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.extension.ExtendedPlanner
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.commands._

/**
 * Strategies for DDL statements execution. This handles DDL commands that
 * are part of SAP Spark extensions.
 *
 * @see [[SparkStrategies#DDLStrategy]]
 */
private[sql] case class SapDDLStrategy(planner: ExtendedPlanner) extends Strategy {

  // scalastyle:off cyclomatic.complexity
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan.flatMap({
    // TODO (AC) Remove this once table-valued function are rebased on top.
    case DescribeRelationCommand(name) => ExecutedCommand(
      DescribeRunnableCommand(planner.optimizedPlan(name))) :: Nil
    case DescribeQueryCommand(query) =>
      val analyzedPlan = planner.analyze(query)
      ExecutedCommand(DescribeRunnableCommand(analyzedPlan)) :: Nil
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
    case ShowTablesUsingCommand(provider, options) =>
      ExecutedCommand(ShowTablesUsingRunnableCommand(provider, options)) :: Nil
    case RegisterAllTablesUsing(provider, options, ignoreConflicts) =>
      ExecutedCommand(RegisterAllTablesCommand(provider, options, ignoreConflicts)) :: Nil
    case RegisterTableUsing(tableName, provider, options, ignoreConflicts) =>
      ExecutedCommand(RegisterTableCommand(tableName, provider, options, ignoreConflicts)) :: Nil
    case DescribeDatasource(unresolvedRelation) =>
      val relation = planner.optimizedRelationLookup(unresolvedRelation)
                            .map(_.asInstanceOf[LogicalRelation]
                                  .relation.asInstanceOf[DescribableRelation])
      ExecutedCommand(DescribeDatasourceCommand(relation)) :: Nil
    case CreatePartitioningFunction(options, name, datatypes, definition, partitionsNo, provider) =>
      ExecutedCommand(CreatePartitioningFunctionCommand(options, name, datatypes, definition,
        partitionsNo, provider)) :: Nil
    case cv@CreateViewCommand(name, temp, query) =>
      ExecutedCommand(cv) :: Nil
    case cmd@UseStatementCommand(input) =>
      ExecutedCommand(cmd) :: Nil
    case CreatePersistentViewCommand(viewIdentifier, plan, provider, options, allowExisting) =>
      ExecutedCommand(CreatePersistentViewRunnableCommand(viewIdentifier, plan, provider,
        options, allowExisting)) :: Nil
    case DropPersistentViewCommand(viewIdentifier, provider, options, allowNotExisting) =>
      ExecutedCommand(DropPersistentViewRunnableCommand(viewIdentifier, provider,
        options, allowNotExisting)) :: Nil
    case _ => Nil
  }).headOption.toSeq
  // scalastyle:on cyclomatic.complexity

}
