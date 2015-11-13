package org.apache.spark.sql.execution

import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.{SQLContext, Strategy}

private[sql] trait BaseSparkPlanner {
  self: SQLContext#SparkPlanner with SparkStrategies =>

  def baseStrategies: Seq[Strategy] =
    DataSourceStrategy ::
      DDLStrategy ::
      TakeOrderedAndProject ::
      HashAggregation ::
      Aggregation ::
      LeftSemiJoin ::
      EquiJoinSelection ::
      InMemoryScans ::
      BasicOperators ::
      CartesianProduct ::
      BroadcastNestedLoopJoin :: Nil
}
