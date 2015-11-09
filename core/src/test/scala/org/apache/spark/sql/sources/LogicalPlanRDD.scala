package org.apache.spark.sql.sources

import org.apache.spark.{TaskContext, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class LogicalPlanRDD(val plan: LogicalPlan, @transient val sc: SparkContext)
  extends RDD[Row](sc, Nil) {
  def getPartitions: Array[org.apache.spark.Partition] = Array()
  def compute(p: org.apache.spark.Partition, ctx: TaskContext): Iterator[Row] = Nil.iterator
}
