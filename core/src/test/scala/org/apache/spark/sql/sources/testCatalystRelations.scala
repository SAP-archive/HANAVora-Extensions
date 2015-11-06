package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

private class DummyRelation(
                             override val schema: StructType,
                             @transient override val sqlContext: SQLContext)
  extends BaseRelation

class DummyCatalystRelation(
                             override val schema: StructType,
                             @transient override val sqlContext: SQLContext)
  extends BaseRelation
  with CatalystSource
  with Serializable {

  @transient
  var isMultiplePartitionExecutionFunc: Seq[CatalystSource] => Boolean = (r) => false
  override def isMultiplePartitionExecution(relations: Seq[CatalystSource]): Boolean =
    isMultiplePartitionExecutionFunc(relations)

  @transient
  var supportsLogicalPlanFunc: LogicalPlan => Boolean = (plan) => true
  override def supportsLogicalPlan(plan: LogicalPlan): Boolean =
    supportsLogicalPlanFunc(plan)

  @transient
  var supportsExpressionFunc: Expression => Boolean = (expr) => true
  override def supportsExpression(expr: Expression): Boolean =
    supportsExpressionFunc(expr)

  @transient
  var logicalPlanToRDDFunc: LogicalPlan => RDD[Row] =
    (plan) => new LogicalPlanRDD(plan, sqlContext.sparkContext)
  override def logicalPlanToRDD(plan: LogicalPlan): RDD[Row] =
    logicalPlanToRDDFunc(plan)

}
