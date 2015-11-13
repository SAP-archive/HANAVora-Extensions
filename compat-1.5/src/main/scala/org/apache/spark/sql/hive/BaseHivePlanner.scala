package org.apache.spark.sql.hive

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.execution.datasources.DataSourceStrategy

private[hive] trait BaseHivePlanner extends HiveStrategies {
  self: HiveContext#SparkPlanner =>

  def baseStrategies(hiveContext: HiveContext): Seq[Strategy] =
    Seq(
      DataSourceStrategy,
      HiveCommandStrategy(hiveContext),
      HiveDDLStrategy,
      DDLStrategy,
      TakeOrderedAndProject,
      InMemoryScans,
      HiveTableScans,
      DataSinks,
      Scripts,
      HashAggregation,
      Aggregation,
      LeftSemiJoin,
      EquiJoinSelection,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin
    )
}
