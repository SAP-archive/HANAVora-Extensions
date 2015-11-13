package org.apache.spark.sql.hive

import org.apache.spark.sql.sources.DataSourceStrategy
import org.apache.spark.sql.{SQLContext, Strategy}

private[hive] trait BaseHivePlanner extends HiveStrategies {
  self: SQLContext#SparkPlanner =>

  def baseStrategies(hiveContext: HiveContext): Seq[Strategy] =
    DataSourceStrategy ::
      DDLStrategy ::
      TakeOrdered ::
      HashAggregation ::
      LeftSemiJoin ::
      HashJoin ::
      InMemoryScans ::
      ParquetOperations ::
      BasicOperators ::
      CartesianProduct ::
      BroadcastNestedLoopJoin :: Nil
}
