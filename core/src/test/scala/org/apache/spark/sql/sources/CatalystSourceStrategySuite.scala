package org.apache.spark.sql.sources

import com.sap.spark.{GlobalSparkContext, PlanTest}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.GlobalSapSQLContext
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Hierarchy, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{CatalystSourceStrategy, CreateLogicalRelation}
import org.apache.spark.sql.types.NodeType
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class CatalystSourceStrategySuite
  extends FunSuite
  with PlanTest
  with GlobalSparkContext
  with GlobalSapSQLContext
  with BeforeAndAfterEach {

  private abstract class CatalystRelation extends BaseRelation with CatalystSource

  private def getLogicalPlans(rdd: RDD[_]): Seq[LogicalPlan] =
    rdd match {
      case lprdd: LogicalPlanRDD =>
        lprdd.plan :: Nil
      case _ => rdd.dependencies.map(_.rdd).flatMap(getLogicalPlans)
    }

  private var nonCatalystRelation: BaseRelation = _
  private var lncr: LogicalPlan = _
  private def lncrCInt: Attribute = lncr.output.find(_.name == "c_int").get
  private var catalystRelation: DummyCatalystRelation = _
  private var lcr: LogicalPlan = _
  private val schema = StructType(Seq(
    StructField("c_int", IntegerType)
  ))
  private def lcrCInt: Attribute = lcr.output.find(_.name == "c_int").get

  override def beforeEach(): Unit = {
    super.beforeEach()

    SparkPlan.currentContext.set(sqlContext)

    nonCatalystRelation = new DummyRelation(schema, sqlc)
    lncr = CreateLogicalRelation(nonCatalystRelation)

    catalystRelation = new DummyCatalystRelation(schema, sqlc)
    lcr = CreateLogicalRelation(catalystRelation)
  }


  test("Non-partitioned push down") {
    catalystRelation.isMultiplePartitionExecutionFunc = x => false
    catalystRelation.supportsLogicalPlanFunc = x => true

    var plan: LogicalPlan = lcr
    var physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(plan, getLogicalPlans(physicals.head.execute()).head)

    plan = lcr.groupBy(lcrCInt)(lcrCInt)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(plan, getLogicalPlans(physicals.head.execute()).head)
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

  test("Unsupported plan reject") {
    catalystRelation.isMultiplePartitionExecutionFunc = x => true
    catalystRelation.supportsLogicalPlanFunc = x => false
    val plan: LogicalPlan = lcr
    val physicals = CatalystSourceStrategy(plan)
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
    val lp = getLogicalPlans(physicals.head.execute()).head
    val allAliases = lp
      .collect({ case p => p.expressions }).flatten
      .filter(_.isInstanceOf[Alias]).map(_.asInstanceOf[Alias])
    val partialCountId = allAliases.find(_.name startsWith "PartialCount").get.exprId.id
    val partialSumId = allAliases.find(_.name startsWith "PartialSum").get.exprId.id
    comparePlans(lcr
      .groupBy(lcrCInt)(lcrCInt,
        count(lcrCInt).as(Symbol(s"PartialCount$partialCountId")),
        sum(lcrCInt).as(Symbol(s"PartialSum$partialSumId"))),
      getLogicalPlans(physicals.head.execute()).head
    )

    plan = lcr.groupBy(lcrCInt)(avg(lcrCInt).as('c_avg), lcrCInt)
      .groupBy(lcrCInt)(lcrCInt)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)

    plan = Hierarchy(lcr, "v",
      expressions.EqualTo(lcrCInt, lcrCInt),
      Nil,
      None,
      AttributeReference("node", NodeType)()
    )
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)
  }

}
