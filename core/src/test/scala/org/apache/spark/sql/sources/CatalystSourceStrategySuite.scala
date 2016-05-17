package org.apache.spark.sql.sources

import com.sap.spark.{GlobalSparkContext, PlanTest}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.{GlobalSapSQLContext, SQLContext}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{AdjacencyListHierarchySpec, Hierarchy, LogicalPlan, PartialAggregation}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{CatalystSourceStrategy, CreateLogicalRelation}
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

  /**
    * A Spark Plan can only be executed if there are converters to and from unsafe rows in
    *
    * This convenience method calls the prepare for execution step, to keep that simple in the test
    *
    * @param plan
    * @return
    */
  private def prepareExecution(plan: SparkPlan): SparkPlan =
    sqlc.prepareForExecution.execute(plan)

  private var nonCatalystRelation: BaseRelation = _
  private var lncr: LogicalPlan = _
  private def lncrCInt: Attribute = lncr.output.find(_.name == "c_int").get
  private def lncrCString: Attribute = lncr.output.find(_.name == "c_string").get
  private var catalystRelation: DummyCatalystRelation = _
  private var lcr: LogicalPlan = _
  private val schema = StructType(Seq(
    StructField("c_int", IntegerType), StructField("c_string", StringType)
  ))
  private def lcrCInt: Attribute = lcr.output.find(_.name == "c_int").get
  private def lcrCString: Attribute = lcr.output.find(_.name == "c_string").get

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    // make sure the current sql context is this
    SQLContext.setActive(sqlc)

    nonCatalystRelation = new DummyRelation(schema, sqlc)
    lncr = CreateLogicalRelation(nonCatalystRelation)

    catalystRelation = new DummyCatalystRelation(schema, sqlc)
    lcr = CreateLogicalRelation(catalystRelation)
  }

  test("Group by with functions") {
    catalystRelation.isMultiplePartitionExecutionFunc = x => true
    catalystRelation.supportsLogicalPlanFunc = x => true

    /**
      * This tests check if the aliasing is correctly handled:
      *
      * The groupBy function adds an alias, and the Partial Aggregation matcher has to find that one
      * and use it correctly. Specifically, it has to re-use it instead of creating a new one.
      */
    var plan: LogicalPlan = lcr.groupBy(Lower(lcrCString))(Lower(lcrCString))
    val correctAliased: Boolean = plan match {
      case partialAgg@PartialAggregation(
      finalGroupings,
      finalAggregates,
      partialGroupings,
      partialAggregates,
      aggregateFunctionToAttributeMap,
      resultExpressions,
      child) => {
        partialGroupings.head.asInstanceOf[AttributeReference].name ==
          partialAggregates.head.asInstanceOf[Alias].name
      }
      case _ => false
    }

    assert(correctAliased)
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
    comparePlans(plan, getLogicalPlans(prepareExecution(physicals.head).execute()).head)

    plan = lcr.limit(1)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(plan, getLogicalPlans(prepareExecution(physicals.head).execute()).head)

    plan = lcr.sortBy(lcrCInt.desc).limit(1)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(plan, getLogicalPlans(prepareExecution(physicals.head).execute()).head)

    plan = lcr.groupBy(lcrCInt)(lcrCInt)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(plan, getLogicalPlans(prepareExecution(physicals.head).execute()).head)

    // test partial average pushdown
    plan = lcr.groupBy(lcrCInt)(avg(lcrCInt).as('c_avg))
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(lcr
      .groupBy(lcrCInt)(
        AggregateExpression(Sum(Cast(lcrCInt, DoubleType)), Partial, false).as("sumc_avg"),
        AggregateExpression(Count(lcrCInt), Partial, false).as("countc_avg")),
      getLogicalPlans(prepareExecution(physicals.head).execute()).head
    )

    plan = lcr.groupBy(lcrCInt)(count(lcrCInt).as('c_cnt))
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(lcr
      .groupBy(lcrCInt)(
        AggregateExpression(Count(lcrCInt), Partial, false).as("countc_cnt")),
      getLogicalPlans(prepareExecution(physicals.head).execute()).head
    )

    plan = lcr.groupBy(lcrCInt)(sum(lcrCInt).as('c_sum))
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(lcr
      .groupBy(lcrCInt)(
        AggregateExpression(Sum(lcrCInt), Partial, false).as("sumc_sum")),
      getLogicalPlans(prepareExecution(physicals.head).execute()).head
    )

    plan = lcr.groupBy(lcrCInt)(max(lcrCInt).as('c_max))
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(lcr
      .groupBy(lcrCInt)(
        AggregateExpression(Max(lcrCInt), Partial, false).as("maxc_max")),
      getLogicalPlans(prepareExecution(physicals.head).execute()).head
    )

    plan = lcr.groupBy(lcrCInt)(min(lcrCInt).as('c_min))
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(lcr
      .groupBy(lcrCInt)(
        AggregateExpression(Min(lcrCInt), Partial, false).as("minc_min")),
      getLogicalPlans(prepareExecution(physicals.head).execute()).head
    )

    plan = lcr.groupBy(lcrCInt)(avg(lcrCInt).as('c_avg), lcrCInt)
      .groupBy(lcrCInt)(lcrCInt)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)

    plan = Hierarchy(
      AdjacencyListHierarchySpec(lcr, "v", expressions.EqualTo(lcrCInt, lcrCInt), None, Nil),
      AttributeReference("node", NodeType)()
    )
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)
  }

}
