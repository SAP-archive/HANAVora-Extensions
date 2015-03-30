package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[Exchange]] Operators where required.
 */
private[sql] class AddDefaultExchange(override val sqlContext: SQLContext)
  extends AddExchange(sqlContext) {

  override def numPartitions = sqlContext.sparkContext.defaultParallelism
}
