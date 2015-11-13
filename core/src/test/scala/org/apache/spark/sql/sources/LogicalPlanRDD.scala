package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.{SparkContext, TaskContext}

class LogicalPlanRDD(val plan: LogicalPlan, @transient val sc: SparkContext)
  extends RDD[Row](sc, Nil) {
  def getPartitions: Array[org.apache.spark.Partition] = Array()
  def compute(p: org.apache.spark.Partition, ctx: TaskContext): Iterator[Row] = Nil.iterator
}
