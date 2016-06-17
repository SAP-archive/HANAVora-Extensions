package org.apache.spark.sql.catalyst.optimizer

import com.sap.spark.PlanTest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, AttributeReference, EqualTo, SelfJoin}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, GlobalSapSQLContext, Row, SQLContext}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.catalyst.optimizer.dsls._
import org.scalatest.FunSuite

// All tests in this suite refer to bug 114127
class SelfJoinsSupportSuite
  extends FunSuite
  with GlobalSapSQLContext
  with PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    private val MAX_ITERATIONS = 100

    val batches =
      Batch("SelfJoinsSupport", FixedPoint(MAX_ITERATIONS), SelfJoinsOptimizer) :: Nil
  }

  val t0 = new LogicalRelation(new BaseRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id", IntegerType)))
  })

  val t1 = new LocalRelation(output = StructType(Seq(StructField("id", IntegerType))).toAttributes)

  val t2 = new LogicalRelation(new BaseRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id", IntegerType)))
  })

  // Join attributes
  val t0id0 = t0.output.find(_.name == "id").get
  val t0id1 = t0.output.find(_.name == "id").get
  val t1id0 = t1.output.find(_.name == "id").get
  val t1id1 = t1.output.find(_.name == "id").get
  val t2id = t2.output.find(_.name == "id").get

  test("Self-joins are properly detected and replaced with the custom node") {
    val originalAnalyzedQuery1 = t0
      .join(t0, joinType = Inner, condition = Some(t0id0 === t0id1)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    // check the plan
    assert(optimized1.isInstanceOf[SelfJoin])
    val selfJoin = optimized1.asInstanceOf[SelfJoin]
    assert(selfJoin.left.asInstanceOf[LogicalRelation].relation == t0.relation)
    assert(selfJoin.right.asInstanceOf[LogicalRelation].relation == t0.relation)
    assert(selfJoin.joinType == Inner)
    assert(selfJoin.condition == Some(t0id0 === t0id1))
    assert(selfJoin.leftPath.asInstanceOf[LogicalRelation].relation == t0.relation)
    assert(selfJoin.rightPath.asInstanceOf[LogicalRelation].relation == t0.relation)

    val originalAnalyzedQuery2 = t1
      .join(t1, joinType = Inner, condition = Some(t1id0 === t1id1)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t1.selfjoin(t1, t1, t1, joinType = Inner,
      condition = Some(t1id0 === t1id1)).analyze

    // check the plan
    comparePlans(correctAnswer2, optimized2)
  }

  test("Self-joins are not replaced for plans with binary operators in subtrees") {
    val originalAnalyzedQuery1 = t0
      .join(t0.unionAll(t0), joinType = Inner, condition = Some(t0id0 === t0id1)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    // check whether there were no changes made to the plan
    comparePlans(originalAnalyzedQuery1, optimized1)
  }

  test("Joins without a common subtree part are not replaced") {
    val originalAnalyzedQuery1 = t0
      .join(t2, joinType = Inner, condition = Some(t0id0 === t2id)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    // check whether there were no changes made to the plan
    comparePlans(originalAnalyzedQuery1, optimized1)
  }

  test("Optimized self-joins for a plan with intermediate nodes " +
    "are correctly computed") {
    // Build an example plan with intermediate projections
    val r1 = new DummyRelation1(sqlc)
    val r2 = new DummyRelation2(sqlc)
    val id1 = AttributeReference("id", IntegerType)(r1.output.head.exprId)
    val id2 = AttributeReference("id", IntegerType)(r2.output.head.exprId)
    val rp = Project(Seq(id1), r1)
    val lp = Project(Seq(id2), r2)

    // Inner join
    val innerJoin = Join(lp, rp, Inner, Some(EqualTo(id1, id2)))
    val planWithInnerJoin = Project(Seq(id1, id2), innerJoin)
    val df1 = DataFrame(sqlContext, planWithInnerJoin)
    assert(df1.queryExecution.optimizedPlan.asInstanceOf[Project].child.isInstanceOf[SelfJoin])
    assertResult(Set(Row(1, 1), Row(2, 2)))(df1.collect().toSet)

    // Left outer join
    val leftJoin = Join(lp, rp, LeftOuter, Some(EqualTo(id1, id2)))
    val planWithLeftJoin = Project(Seq(id1, id2), leftJoin)
    val df2 = DataFrame(sqlContext, planWithLeftJoin)
    assert(df2.queryExecution.optimizedPlan.asInstanceOf[Project].child.isInstanceOf[SelfJoin])
    assertResult(Set(Row(1, 1), Row(2, 2), Row(null, null)))(df2.collect().toSet)

    // Right outer join
    val rightJoin = Join(lp, rp, RightOuter, Some(EqualTo(id1, id2)))
    val planWithRightJoin = Project(Seq(id1, id2), rightJoin)
    val df3 = DataFrame(sqlContext, planWithRightJoin)
    assert(df3.queryExecution.optimizedPlan.asInstanceOf[Project].child.isInstanceOf[SelfJoin])
    assertResult(Set(Row(1, 1), Row(2, 2), Row(null, null)))(df3.collect().toSet)

    // Full outer join
    val fullJoin = Join(lp, rp, FullOuter, Some(EqualTo(id1, id2)))
    val planWithFullJoin = Project(Seq(id1, id2), fullJoin)
    val df4 = DataFrame(sqlContext, planWithFullJoin)
    assert(df4.queryExecution.optimizedPlan.asInstanceOf[Project].child.isInstanceOf[SelfJoin])
    assertResult(Set(Row(1, 1), Row(2, 2), Row(null, null)))(df4.collect().toSet)

    // Left semi join
    val semiJoin = Join(lp, rp, LeftSemi, Some(EqualTo(id1, id2)))
    val planWithSemiJoin = Project(Seq(id2), semiJoin)
    val df5 = DataFrame(sqlContext, planWithSemiJoin)
    assert(df5.queryExecution.optimizedPlan.asInstanceOf[Project].child.isInstanceOf[SelfJoin])
    assertResult(Set(Row(1), Row(2)))(df5.collect().toSet)
  }

  test("Self-joins are properly detected and replaced with unequal subtrees") {
    val r1 = new DummyRelation1(sqlc)
    val r2 = new DummyRelation2(sqlc)
    val id1 = AttributeReference("id", IntegerType)(r1.output.head.exprId)
    val id2 = AttributeReference("id", IntegerType)(r2.output.head.exprId)
    val rp1 = Project(Seq(id1), r1)
    val lp = Project(Seq(id2), r2)
    val ra = Aggregate(Seq(id1), Seq(id1), rp1)
    val rp2 = Project(Seq(id1), ra)
    val innerJoin = Join(lp, rp2, Inner, Some(EqualTo(id1, id2)))
    val planWithInnerJoin = Project(Seq(id1, id2), innerJoin)
    val df = DataFrame(sqlContext, planWithInnerJoin)
    assert(df.queryExecution.optimizedPlan.asInstanceOf[Project].child.isInstanceOf[SelfJoin])
    assertResult(Set(Row(1, 1), Row(2, 2)))(df.collect().toSet)
  }

}

// Test logical relations classes
class DummyRelation1(sqlc: SQLContext)
  extends LogicalRelation(new BaseRelation with TableScan with Serializable {

    override def sqlContext: SQLContext = sqlc

    override def schema: StructType = StructType(Seq(StructField("id", IntegerType)))

    override def buildScan(): RDD[Row] =
      sqlc.sparkContext.parallelize(Row(1) :: Row(2) :: Row(null) :: Nil)

    override def equals(obj: scala.Any): Boolean = true

    override def hashCode(): Int = 0

  })

class DummyRelation2(sqlc: SQLContext)
  extends LogicalRelation(new BaseRelation with TableScan with Serializable {

    override def sqlContext: SQLContext = sqlc

    override def schema: StructType = StructType(Seq(StructField("id", IntegerType)))

    override def buildScan(): RDD[Row] =
      sqlc.sparkContext.parallelize(Row(1) :: Row(2) :: Row(null) :: Nil)

    override def equals(obj: scala.Any): Boolean = true

    override def hashCode(): Int = 0

  })

package object dsls {

  implicit class CustomDsls(val logicalPlan: LogicalPlan) {

    def selfjoin(other: LogicalPlan,
                 leftPath: LogicalPlan,
                 rightPath: LogicalPlan,
                 joinType: JoinType = Inner,
                 condition: Option[Expression] = None): LogicalPlan =
      SelfJoin(logicalPlan, other, joinType, condition, leftPath, rightPath)
  }

}
