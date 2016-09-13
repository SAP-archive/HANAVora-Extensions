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
import org.apache.spark.sql.execution.aggregate.TungstenAggregate
import org.apache.spark.sql.execution.datasources.{CatalystSourceStrategy, LogicalRelation}
import org.apache.spark.sql.sources.sql.SqlLikeRelation
import org.apache.spark.sql.types._
import org.apache.spark.util.DummyRelationUtils._
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

  implicit class RichLogicalPlan(plan: LogicalPlan) {
    def attribute(name: String): Attribute =
      plan.output.find(_.name == name).get
  }

  private val schema = StructType(Seq(
    'c_int.ofType.int,
    'c_string.ofType.int,
    'c_int2.ofType.int
  ))

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    // make sure the current sql context is this
    SQLContext.setActive(sqlc)
  }

  test("Group by with functions") {
    val catalystRelation = DummyCatalystSourceRelation(schema)(sqlc)
    val lcr = new LogicalRelation(catalystRelation)

    /**
      * This tests check if the aliasing is correctly handled:
      *
      * The groupBy function adds an alias, and the Partial Aggregation matcher has to find that one
      * and use it correctly. Specifically, it has to re-use it instead of creating a new one.
      */
    val plan: LogicalPlan =
      lcr.groupBy(Lower(lcr.attribute("c_string")))(Lower(lcr.attribute("c_string")))
    val correctAliased: Boolean = plan match {
      case partialAgg@PartialAggregation(
      finalGroupings,
      finalAggregates,
      partialGroupings,
      partialAggregates,
      aggregateFunctionToAttributeMap,
      resultExpressions,
      child) => {
        partialAggregates.head.find(_.toString == partialGroupings.head.name).isDefined
      }
      case _ => false
    }

    assert(correctAliased)
  }


  test("Non-partitioned push down") {
    val catalystRelation =
      DummyCatalystSourceRelation(
        schema,
        isMultiplePartitionExecutionFunc = Some(_ => false))(sqlc)
    val lcr = LogicalRelation(catalystRelation)
    val lcrCInt = lcr.attribute("c_int")

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
    val catalystRelation =
      DummyCatalystSourceRelation(
        schema,
        isMultiplePartitionExecutionFunc = Some(_ => false),
        supportsLogicalPlanFunc = Some(_ => false))(sqlc)
    val lcr = LogicalRelation(catalystRelation)
    val lcrCInt = lcr.attribute("c_int")

    var plan: LogicalPlan = lcr
    var physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)

    plan = lcr.groupBy(lcrCInt)(lcrCInt)
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)
  }

  test("Non-catalyst relation reject") {
    val lcr = LogicalRelation(DummyCatalystSourceRelation(schema)(sqlc))
    val lncr = LogicalRelation(DummyRelation(schema)(sqlc))
    val lncrCInt = lncr.attribute("c_int")
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
    val catalystRelation =
      DummyCatalystSourceRelation(
        schema,
        isMultiplePartitionExecutionFunc = Some(_ => true),
        supportsLogicalPlanFunc = Some(_ => false))(sqlc)
    val plan: LogicalPlan = LogicalRelation(catalystRelation)
    val physicals = CatalystSourceStrategy(plan)
    assert(physicals.isEmpty)
  }

  test("Partitioned push down") {
    val catalystRelation =
      DummyCatalystSourceRelation(
        schema,
        isMultiplePartitionExecutionFunc = Some(_ => true),
        supportsLogicalPlanFunc = Some(_ => true))(sqlc)
    val lcr = LogicalRelation(catalystRelation)
    val lcrCInt = lcr.attribute("c_int")
    val lcrC2Int = lcr.attribute("c_int2")
    val lcrCString = lcr.attribute("c_string")

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
        AggregateExpression(Sum(Cast(lcrCInt, DoubleType)), Partial, false).as("c_avg_sum"),
        AggregateExpression(Count(lcrCInt), Partial, false).as("c_avg_count")),
      getLogicalPlans(prepareExecution(physicals.head).execute()).head
    )

    plan = lcr.groupBy(lcrCInt)(count(lcrCInt).as('c_cnt))
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(lcr
      .groupBy(lcrCInt)(
        AggregateExpression(Count(lcrCInt), Partial, false).as("c_cnt_count")),
      getLogicalPlans(prepareExecution(physicals.head).execute()).head
    )

    plan = lcr.groupBy(lcrCInt)(sum(lcrCInt).as('c_sum))
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(lcr
      .groupBy(lcrCInt)(
        AggregateExpression(Sum(lcrCInt), Partial, false).as("c_sum_sum")),
      getLogicalPlans(prepareExecution(physicals.head).execute()).head
    )

    plan = lcr.groupBy(lcrCInt)(max(lcrCInt).as('c_max))
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(lcr
      .groupBy(lcrCInt)(
        AggregateExpression(Max(lcrCInt), Partial, false).as("c_max_max")),
      getLogicalPlans(prepareExecution(physicals.head).execute()).head
    )

    plan = lcr.groupBy(lcrCInt)(min(lcrCInt).as('c_min))
    physicals = CatalystSourceStrategy(plan)
    assert(physicals.nonEmpty)
    comparePlans(lcr
      .groupBy(lcrCInt)(
        AggregateExpression(Min(lcrCInt), Partial, false).as("c_min_min")),
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

    plan = lcr.groupBy(lcrCString)(min(lcrCInt), min(lcrC2Int))
    physicals = CatalystSourceStrategy(plan)
  }

  test("Partial aggregate names are non-ambiguous") {
    val catalystRelation =
      new DummyCatalystSourceRelation(
        schema,
        isMultiplePartitionExecutionFunc = Some(_ => true),
        supportsLogicalPlanFunc = Some(_ => true))(sqlc)
        with Table
        with SqlLikeRelation {

        override def relationName: String = "foo"

        override def isTemporary: Boolean = true
      }
    val lcr = LogicalRelation(catalystRelation)
    val lcrCInt = lcr.attribute("c_int")
    val lcrC2Int = lcr.attribute("c_int2")
    val lcrCString = lcr.attribute("c_string")

    val plan = lcr.groupBy(lcrCString)(min(lcrCInt).as("c1min"), min(lcrC2Int).as("c2min"))
    plan match {
      case PartialAggregation(_, _, _, pushdownExpressions, _, _, _) =>
        val distinctPushdownNames = pushdownExpressions.map(_.name).distinct
        assert(distinctPushdownNames.size == pushdownExpressions.size)
      case _ => fail("Could not calculate partial aggregation")
    }
  }

}
