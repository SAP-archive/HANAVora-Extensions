package org.apache.spark.sql.execution

import org.apache.spark.sql.sources.DataSourceStrategy
import org.apache.spark.sql.{SQLContext, Strategy}

private[sql] trait BaseSparkPlanner {
  self: SQLContext#SparkPlanner with SparkStrategies =>

  def baseStrategies: Seq[Strategy] =
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
