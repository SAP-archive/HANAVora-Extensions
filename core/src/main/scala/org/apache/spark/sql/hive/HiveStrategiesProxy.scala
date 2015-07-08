package org.apache.spark.sql.hive

import org.apache.spark.sql.SQLContext

/*
 * TODO: This is a workaround to HiveStrategies not being visible from org.apache.spark.sql package.
 */
private[sql] trait HiveStrategiesProxy extends HiveStrategies {
  self: SQLContext#SparkPlanner =>

}
