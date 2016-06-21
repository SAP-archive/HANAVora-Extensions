package org.apache.spark.sql.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
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
  // scalastyle:off method.length
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
    case ShowDatasourceTablesCommand(provider, options) =>
      ExecutedCommand(ShowDataSourceTablesRunnableCommand(provider, options)) :: Nil
    case ShowTablesUsingCommand(provider, options) =>
      ExecutedCommand(ShowTablesUsingRunnableCommand(provider, options)) :: Nil
    case RegisterAllTablesUsing(provider, options, ignoreConflicts) =>
      ExecutedCommand(RegisterAllTablesCommand(provider, options, ignoreConflicts)) :: Nil
    case RegisterTableUsing(tableName, provider, options, ignoreConflicts) =>
      ExecutedCommand(RegisterTableCommand(tableName, provider, options, ignoreConflicts)) :: Nil
    case d: DeepDescribeCommand =>
      ExecutedCommand(d) :: Nil
    case CreateHashPartitioningFunction(options, name, provider, datatypes, partitionsNo) =>
      ExecutedCommand(CreateHashPartitioningFunctionCommand(options, name, datatypes, partitionsNo,
        provider)) :: Nil
    case CreateRangeSplittersPartitioningFunction(options, name, provider, datatype, splitters,
    rightClosed) =>
      ExecutedCommand(CreateRangeSplitPartitioningFunctionCommand(options, name, datatype,
        splitters, rightClosed, provider)) :: Nil
    case CreateRangeIntervalPartitioningFunction(options, name, provider, datatype, start, end,
    strideParts) =>
      ExecutedCommand(CreateRangeIntervalPartitioningFunctionCommand(options, name, datatype,
        start, end, strideParts, provider)) :: Nil
    case DropPartitioningFunction(options, name, provider, allowNotExisting) =>
      ExecutedCommand(DropPartitioningFunctionCommand(options, name, allowNotExisting,
        provider)) :: Nil
    case cmd@UseStatementCommand(input) =>
      ExecutedCommand(cmd) :: Nil
    case DescribeTableUsingCommand(provider, name, options) =>
      ExecutedCommand(DescribeTableUsingRunnableCommand(provider, name, options)) :: Nil
    case RawDDLCommand(
      identifier, objectType, statementType, sparkSchema, sqlCommand, provider, options) =>
      ExecutedCommand(RawDDLRunnableCommand(
        identifier, objectType, statementType, sparkSchema, sqlCommand, provider, options)) :: Nil
    case _ => Nil
  }).headOption.toSeq
  // scalastyle:on cyclomatic.complexity

}
