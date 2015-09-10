package org.apache.spark.sql.sources

import com.sap.spark.{GlobalSparkContext, PlanTest}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Expression, Attribute, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{GlobalVelocitySQLContext, SQLContext}
import org.apache.spark.{SparkContext, TaskContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class CatalystSourceStrategySuite
  extends FunSuite
  with PlanTest
  with GlobalSparkContext
  with GlobalVelocitySQLContext
  with BeforeAndAfterEach {

  private abstract class CatalystRelation extends BaseRelation with CatalystSource

  private def getLogicalPlans(rdd: RDD[_]): Seq[LogicalPlan] =
    rdd match {
      case lprdd: LogicalPlanRDD =>
        lprdd.plan :: Nil
      case _ => rdd.dependencies.map(_.rdd).flatMap(getLogicalPlans)
    }

  private var nonCatalystRelation: BaseRelation = _
  private var lncr: LogicalRelation = _
  private def lncrCInt: Attribute = lncr.output.find(_.name == "c_int").get
  private var catalystRelation: DummyCatalystRelation = _
  private var lcr: LogicalRelation = _
  private val schema = StructType(Seq(
    StructField("c_int", IntegerType)
  ))
  private def lcrCInt: Attribute = lcr.output.find(_.name == "c_int").get

  override def beforeEach(): Unit = {
    super.beforeEach()

    SparkPlan.currentContext.set(sqlContext)

    nonCatalystRelation = new DummyRelation(schema, sqlc)
    lncr = LogicalRelation(nonCatalystRelation)

    catalystRelation = new DummyCatalystRelation(schema, sqlc)
    lcr = LogicalRelation(catalystRelation)
  }


  test("Non-partitioned push down") {
    catalystRelation.isMultiplePartitionExecutionFunc = x => false
    catalystRelation.supportsLogicalPlanFunc = x => true

    var plan: LogicalPlan = lcr
    var physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(plan, physicals.head.execute().asInstanceOf[LogicalPlanRDD].plan)

    plan = lcr.groupBy(lcrCInt)(lcrCInt)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(plan, physicals.head.execute().asInstanceOf[LogicalPlanRDD].plan)
  }

  test("Non-partitioned unsupported reject") {
    catalystRelation.isMultiplePartitionExecutionFunc = x => false
    catalystRelation.supportsLogicalPlanFunc = x => false

    var plan: LogicalPlan = lcr
    var physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)

    plan = lcr.groupBy(lcrCInt)(lcrCInt)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)
  }

  test("Non-catalyst relation reject") {
    var plan: LogicalPlan = lncr
    var physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)

    plan = lncr
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)

    plan = lncr.groupBy(lncrCInt)(lncrCInt)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)

    plan = lncr.join(lcr)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)

    plan = lcr.join(lncr)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)
  }

  test("Partitioned push down") {
    catalystRelation.isMultiplePartitionExecutionFunc = x => true
    catalystRelation.supportsLogicalPlanFunc = x => true

    var plan: LogicalPlan = lcr
    var physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(plan, getLogicalPlans(physicals.head.execute()).head)

    plan = lcr.limit(1)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(plan, getLogicalPlans(physicals.head.execute()).head)

    plan = lcr.sortBy(lcrCInt.desc).limit(1)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(plan, getLogicalPlans(physicals.head.execute()).head)

    plan = lcr.groupBy(lcrCInt)(lcrCInt)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(plan, getLogicalPlans(physicals.head.execute()).head)

    plan = lcr.groupBy(lcrCInt)(avg(lcrCInt).as('c_avg))
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(lcr
      .groupBy(lcrCInt)(lcrCInt,
        count(lcrCInt).as('PartialCount), sum(lcrCInt).as('PartialSum)),
      getLogicalPlans(physicals.head.execute()).head
    )
  }

}

private class DummyRelation(override val schema: StructType,
                            @transient override val sqlContext: SQLContext) extends BaseRelation

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

private class LogicalPlanRDD(val plan: LogicalPlan, @transient val sc: SparkContext)
  extends RDD[Row](sc, Nil) {
  def getPartitions: Array[org.apache.spark.Partition] = Array()
  def compute(p: org.apache.spark.Partition, ctx: TaskContext): Iterator[Row] = Nil.iterator
}
