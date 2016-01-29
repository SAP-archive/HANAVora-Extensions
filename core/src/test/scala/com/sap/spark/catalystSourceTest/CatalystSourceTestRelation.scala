package com.sap.spark.catalystSourceTest

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{TaskContext, Partition, Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.{BaseRelation, CatalystSource}
import org.apache.spark.sql.types.{DataType, StructType}

/**
  * Test Relation for the CatalystSourceTest
  *
  * Implemented for the 1.6 support to increase test coverage
  *
  * The idea is as follows:
  *
  * For a test, you can inject data in [[CatalystSourceTestRDD]] that represents the results of
  * a pushed down partial plan. Thus, it is possible if
  * [[org.apache.spark.sql.catalyst.plans.logical.PartialAggregation]] works as expected.
  *
  */
class CatalystSourceTestRelation(val parameters: Map[String, String],
                                 override val schema: StructType, isTemporary: Boolean,
                                 allowExisting: Boolean) (@transient val sqlContext: SQLContext)
  extends BaseRelation with CatalystSource
{
  /**
    * Returns dummy RDDs for the test.
    *
    * @param plan Logical plan.
    * @return
    */
  override def logicalPlanToRDD(plan: LogicalPlan): RDD[Row] = {
    new CatalystSourceTestRDD()(sqlContext.sparkContext)
  }

  /**
    * We support all the expressions
    *
    * @param expr Expression.
    * @return
    */
  override def supportsExpression(expr: Expression): Boolean = true

  /**
    * CatalystSource Strategy already does some checks for global operations,
    * so we can safely return true here
    *
    * @param plan Logical plan.
    * @return
    */
  override def supportsLogicalPlan(plan: LogicalPlan): Boolean = true

  /**
    * Assuming multi partitions
    *
    * @param relations Relations participating in a query.
    * @return
    */
  override def isMultiplePartitionExecution(relations: Seq[CatalystSource]): Boolean = true
}

object CatalystSourceTestRDD {
  var rowData: Map[CataystSourceTestRDDPartition, Seq[Row]] = Map.empty
  var partitions: Seq[CataystSourceTestRDDPartition] = Seq.empty
}

class CatalystSourceTestRDD(
  val sqlString: String = "")
                           (sc: SparkContext)
  extends RDD[Row](sc, Nil)
    with Logging {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    // scalastyle:off magic.number
    CatalystSourceTestRDD.rowData.get(split.asInstanceOf[CataystSourceTestRDDPartition])
      .getOrElse(throw
        new RuntimeException(s"Test requested non existing data for partition ${split}")).toIterator
  }

  override protected def getPartitions: Array[Partition] =
    CatalystSourceTestRDD.partitions.toArray
}

case class CataystSourceTestRDDPartition(index: Int) extends Partition
