package org.apache.spark.sql.catalyst.optimizer

import com.sap.spark.PlanTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.PhysicalSelfJoin
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.PlanUtils._
import org.apache.spark.sql.{DataFrame, GlobalSapSQLContext, Row, SQLContext}
import org.apache.spark.util.DummyRelationUtils._
import org.scalatest.{FunSuite, Inside}

// All tests in this suite refer to bug 114127
class SelfJoinsSupportSuite
  extends FunSuite
  with GlobalSapSQLContext
  with PlanTest
  with Inside {

  val t0 = LogicalRelation(DummyRelation('id.ofType.int)(sqlc))

  val t1 = LocalRelation(output = 'id.ofType.int.toAttributes)

  val t2 = LogicalRelation(new BaseRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = 'id.ofType.int
  })

  // Join attributes
  val t0id0 = t0.output.find(_.name == "id").get
  val t0id1 = t0.output.find(_.name == "id").get
  val t1id0 = t1.output.find(_.name == "id").get
  val t1id1 = t1.output.find(_.name == "id").get
  val t2id = t2.output.find(_.name == "id").get

  test("Self-joins are properly detected") {
    val originalAnalyzedQuery1 = t0
      .join(t0, joinType = Inner, condition = Some(t0id0 === t0id1)).analyze

    inside(originalAnalyzedQuery1) {
      case SelfJoin(LogicalRelation(lRelation, _), LogicalRelation(rRelation, _), joinType,
                    condition, LogicalRelation(lPath, _), LogicalRelation(rPath, _)) =>
        assertResult(t0.relation)(lRelation)
        assertResult(t0.relation)(rRelation)
        assertResult(Inner)(joinType)
        assertResult(Some(t0id0 === t0id1))(condition)
        assertResult(t0.relation)(lPath)
        assertResult(t0.relation)(rPath)
    }
  }

  test("Self-joins are not replaced for plans with binary operators in subtrees") {
    val originalAnalyzedQuery1 = t0
      .join(t0.unionAll(t0), joinType = Inner, condition = Some(t0id0 === t0id1))
      .analyze
      .asInstanceOf[Join]

    assert(SelfJoin.unapply(originalAnalyzedQuery1).isEmpty)
  }

  test("Joins without a common subtree part are not replaced") {
    val originalAnalyzedQuery1 = t0
      .join(t2, joinType = Inner, condition = Some(t0id0 === t2id))
      .analyze
      .asInstanceOf[Join]

    assert(SelfJoin.unapply(originalAnalyzedQuery1).isEmpty)
  }

  test("Optimized self-joins for a plan with intermediate nodes " +
    "are correctly computed") {
    // Build an example plan with intermediate projections
    val r1 = new DummyLogicalRelation(sqlc)
    val r2 = new DummyLogicalRelation(sqlc)
    val id1 = AttributeReference("id", IntegerType)(r1.output.head.exprId)
    val id2 = AttributeReference("id", IntegerType)(r2.output.head.exprId)
    val rp = Project(Seq(id1), r1)
    val lp = Project(Seq(id2), r2)

    // Inner join
    val innerJoin = Join(lp, rp, Inner, Some(EqualTo(id1, id2)))
    val planWithInnerJoin = Project(Seq(id1, id2), innerJoin)
    val df1 = DataFrame(sqlContext, planWithInnerJoin)
    assert(df1.queryExecution.sparkPlan.exists(_.isInstanceOf[PhysicalSelfJoin]))
    assertResult(Set(Row(1, 1), Row(2, 2)))(df1.collect().toSet)

    // Left outer join
    val leftJoin = Join(lp, rp, LeftOuter, Some(EqualTo(id1, id2)))
    val planWithLeftJoin = Project(Seq(id1, id2), leftJoin)
    val df2 = DataFrame(sqlContext, planWithLeftJoin)
    assert(df2.queryExecution.sparkPlan.exists(_.isInstanceOf[PhysicalSelfJoin]))
    assertResult(Set(Row(1, 1), Row(2, 2), Row(null, null)))(df2.collect().toSet)

    // Right outer join
    val rightJoin = Join(lp, rp, RightOuter, Some(EqualTo(id1, id2)))
    val planWithRightJoin = Project(Seq(id1, id2), rightJoin)
    val df3 = DataFrame(sqlContext, planWithRightJoin)
    assert(df3.queryExecution.sparkPlan.exists(_.isInstanceOf[PhysicalSelfJoin]))
    assertResult(Set(Row(1, 1), Row(2, 2), Row(null, null)))(df3.collect().toSet)

    // Full outer join
    val fullJoin = Join(lp, rp, FullOuter, Some(EqualTo(id1, id2)))
    val planWithFullJoin = Project(Seq(id1, id2), fullJoin)
    val df4 = DataFrame(sqlContext, planWithFullJoin)
    assert(df4.queryExecution.sparkPlan.exists(_.isInstanceOf[PhysicalSelfJoin]))
    assertResult(Set(Row(1, 1), Row(2, 2), Row(null, null)))(df4.collect().toSet)

    // Left semi join
    val semiJoin = Join(lp, rp, LeftSemi, Some(EqualTo(id1, id2)))
    val planWithSemiJoin = Project(Seq(id2), semiJoin)
    val df5 = DataFrame(sqlContext, planWithSemiJoin)
    assert(df5.queryExecution.sparkPlan.exists(_.isInstanceOf[PhysicalSelfJoin]))
    assertResult(Set(Row(1), Row(2)))(df5.collect().toSet)
  }

  test("Self-joins are properly detected and replaced with unequal subtrees") {
    val r1 = new DummyLogicalRelation(sqlc)
    val r2 = new DummyLogicalRelation(sqlc)
    val id1 = AttributeReference("id", IntegerType)(r1.output.head.exprId)
    val id2 = AttributeReference("id", IntegerType)(r2.output.head.exprId)
    val rp1 = Project(Seq(id1), r1)
    val lp = Project(Seq(id2), r2)
    val ra = Aggregate(Seq(id1), Seq(id1), rp1)
    val rp2 = Project(Seq(id1), ra)
    val innerJoin = Join(lp, rp2, Inner, Some(EqualTo(id1, id2)))
    val planWithInnerJoin = Project(Seq(id1, id2), innerJoin)
    val df = DataFrame(sqlContext, planWithInnerJoin)
    assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[PhysicalSelfJoin]))
    assertResult(Set(Row(1, 1), Row(2, 2)))(df.collect().toSet)
  }

}

// scalastyle:off
trait AlwaysEqual {
  override def equals(obj: scala.Any): Boolean = true

  override def hashCode(): Int = 0
}
// scalastyle:on

// Test logical relations classes
class DummyLogicalRelation(sqlc: SQLContext)
  extends LogicalRelation(
    new DummyRelationWithTableScan(
      'id.ofType.int,
      Row(1) :: Row(2) :: Row(null) :: Nil)(sqlc)
      with AlwaysEqual)
