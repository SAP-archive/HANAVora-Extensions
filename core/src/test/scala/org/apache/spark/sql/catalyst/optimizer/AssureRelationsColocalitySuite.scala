package org.apache.spark.sql.catalyst.optimizer

import com.sap.spark.PlanTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{RightOuter, FullOuter, LeftOuter, Inner}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{BaseRelation, PartitionedRelation}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{GlobalSapSQLContext, SQLContext}
import org.scalatest.FunSuite

class AssureRelationsColocalitySuite
  extends FunSuite
  with GlobalSapSQLContext
  with PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    private val MAX_ITERATIONS = 100

    val batches =
      Batch("RelationsColocalityAssurance", FixedPoint(MAX_ITERATIONS),
        AssureRelationsColocality) :: Nil
  }

  // Test relations
  val testRelation0 = new LogicalRelation(new BaseRelation {
    override def sqlContext: SQLContext = sqlc

    override def schema: StructType = StructType(Seq(StructField("id0", IntegerType)))
  })

  val testRelation1 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc

    override def schema: StructType = StructType(Seq(StructField("id1", IntegerType)))

    override def partitioningFunctionColumns: Option[Set[String]] = None

    override def partitioningFunctionName: Option[String] = None
  })

  val testRelation2 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc

    override def schema: StructType = StructType(Seq(StructField("id2", IntegerType)))

    override def partitioningFunctionColumns: Option[Set[String]] =
      Some(Set("id2"))

    override def partitioningFunctionName: Option[String] =
      Some("F1")
  })

  val testRelation3 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc

    override def schema: StructType = StructType(Seq(StructField("id3", IntegerType)))

    override def partitioningFunctionColumns: Option[Set[String]] =
      Some(Set("id3"))

    override def partitioningFunctionName: Option[String] =
      Some("F1")
  })

  // Join attributes
  val t0id = testRelation0.output.find(_.name == "id0").get
  val t1id = testRelation1.output.find(_.name == "id1").get
  val t2id = testRelation2.output.find(_.name == "id2").get
  val t3id = testRelation3.output.find(_.name == "id3").get

  /**
   * This test checks the following re-orderings:
   *
   * Phase 1:
   *        O                    O
   *      /  \                 /  \
   *     O   T3(F)   ===>     T1   O
   *   /  \                      /  \
   *  T1   T2(F)             T2(F)  T3(F)
   *
   * Phase 2:
   *         O                   O
   *       /  \                /  \
   *      O   T3(F)  ===>     T1   O
   *    /  \                     /  \
   * T2(F)  T1               T2(F)  T3(F)
   *
   * Phase 3: check whether for outer joins the plan remains unchanged
   *
   * Legend:
   * T1 - unpartitioned table
   * T2, T3 - two tables partitioned by the same function F
   */
  test("Right rotation on logical plans is correctly applied in case of local co-locality") {
    // Phase 1
    val originalAnalyzedQuery1 = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(t1id === t2id))
      .join(testRelation3, joinType = Inner, condition = Some(t2id === t3id)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = testRelation1
      .join(testRelation2.join(testRelation3, joinType = Inner, condition = Some(t2id === t3id)),
        joinType = Inner, condition = Some(t1id === t2id)).analyze

    comparePlans(optimized1, correctAnswer1)

    // Phase 2 - Inner joins
    val originalAnalyzedQuery2 = testRelation2
      .join(testRelation1, joinType = Inner, condition = Some(t2id === t1id))
      .join(testRelation3, joinType = Inner, condition = Some(t2id === t3id)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = testRelation1
      .join(testRelation2.join(testRelation3, joinType = Inner, condition = Some(t2id === t3id)),
        joinType = Inner, condition = Some(t2id === t1id)).analyze

    comparePlans(optimized2, correctAnswer2)

    // Phase 2 - Outer joins
    val originalAnalyzedQuery3 = testRelation2
      .join(testRelation1, joinType = LeftOuter, condition = Some(t2id === t1id))
      .join(testRelation3, joinType = Inner, condition = Some(t2id === t3id)).analyze

    val optimized3 = Optimize.execute(originalAnalyzedQuery3)

    // The plan should remain unchanged due to the left outer join
    comparePlans(optimized3, originalAnalyzedQuery3)

    val originalAnalyzedQuery4 = testRelation2
      .join(testRelation1, joinType = RightOuter, condition = Some(t2id === t1id))
      .join(testRelation3, joinType = Inner, condition = Some(t2id === t3id)).analyze

    val optimized4 = Optimize.execute(originalAnalyzedQuery4)

    // The plan should remain unchanged due to the right outer join
    comparePlans(optimized4, originalAnalyzedQuery4)

    val originalAnalyzedQuery5 = testRelation2
      .join(testRelation1, joinType = FullOuter, condition = Some(t2id === t1id))
      .join(testRelation3, joinType = RightOuter, condition = Some(t2id === t3id)).analyze

    val optimized5 = Optimize.execute(originalAnalyzedQuery5)

    // The plan should remain unchanged due to the outer joins
    comparePlans(optimized5, originalAnalyzedQuery5)
  }

  /**
   * This test checks the following re-orderings:
   *
   * Phase 1:
   *           O                    O
   *         /  \                 /  \
   *     T3(F)  O       ===>     O   T1
   *          /  \             /  \
   *      T2(F)  T1        T3(F)  T2(F)
   *
   * Phase 2:
   *           O                    O
   *         /  \                 /  \
   *     T3(F)  O       ===>     O   T1
   *          /  \             /  \
   *        T1   T2(F)     T3(F)  T2(F)
   *
   * Phase 3: check whether for outer joins the plan remains unchanged
   *
   * Legend:
   * T1 - unpartitioned table
   * T2, T3 - two tables partitioned by the same function F
   */
  test("Left rotation on logical plans is correctly applied in case of local co-locality") {
    // Phase 1
    val originalAnalyzedQuery1 = testRelation3.join(
      testRelation2.join(testRelation1, joinType = Inner, condition = Some(t2id === t1id)),
      joinType = Inner, condition = Some(t3id === t2id)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = testRelation3
      .join(testRelation2, joinType = Inner, condition = Some(t3id === t2id))
      .join(testRelation1, joinType = Inner, condition = Some(t2id === t1id)).analyze

    comparePlans(optimized1, correctAnswer1)

    // Phase 2 - Inner joins
    val originalAnalyzedQuery2 = testRelation3
      .join(testRelation1.join(testRelation2, joinType = Inner, condition = Some(t1id === t2id)),
        joinType = Inner, condition = Some(t3id === t2id)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = testRelation3
      .join(testRelation2, joinType = Inner, condition = Some(t3id === t2id))
      .join(testRelation1, joinType = Inner, condition = Some(t1id === t2id)).analyze

    comparePlans(optimized2, correctAnswer2)

    // Phase 2 - Outer joins
    val originalAnalyzedQuery3 = testRelation3
      .join(testRelation1
        .join(testRelation2, joinType = FullOuter, condition = Some(t1id === t2id)),
        joinType = Inner, condition = Some(t3id === t2id)).analyze

    val optimized3 = Optimize.execute(originalAnalyzedQuery3)

    // The plan should remain unchanged due to the full outer join
    comparePlans(optimized3, originalAnalyzedQuery3)

    val originalAnalyzedQuery4 = testRelation3
      .join(testRelation1
        .join(testRelation2, joinType = LeftOuter, condition = Some(t1id === t2id)),
        joinType = Inner, condition = Some(t3id === t2id)).analyze

    val optimized4 = Optimize.execute(originalAnalyzedQuery4)

    // The plan should remain unchanged due to the left outer join
    comparePlans(optimized4, originalAnalyzedQuery4)

    val originalAnalyzedQuery5 = testRelation3
      .join(testRelation1
        .join(testRelation2, joinType = RightOuter, condition = Some(t1id === t2id)),
        joinType = Inner, condition = Some(t3id === t2id)).analyze

    val optimized5 = Optimize.execute(originalAnalyzedQuery5)

    // The plan should remain unchanged due to the right outer join
    comparePlans(optimized5, originalAnalyzedQuery5)
  }

  test("The join condition is properly altered in case when the upper join condition " +
    "involves columns from a non-partitioned table") {
    val originalAnalyzedQuery1 = testRelation3.join(
      testRelation2.join(testRelation1, joinType = Inner, condition = Some(t2id === t1id)),
      joinType = Inner, condition = Some(t1id === t3id)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = testRelation3
      .join(testRelation2, joinType = Inner, condition = Some(t2id === t3id))
      .join(testRelation1, joinType = Inner, condition = Some(t2id === t1id)).analyze

    comparePlans(optimized1, correctAnswer1)

    val originalAnalyzedQuery2 = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(t1id === t2id))
      .join(testRelation3, joinType = Inner, condition = Some(t3id === t1id)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = testRelation1
      .join(testRelation2.join(testRelation3, joinType = Inner, condition = Some(t3id === t2id)),
        joinType = Inner, condition = Some(t1id === t2id)).analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("A proper rotation is applied even in case when a join involves tables " +
    "which do not provide partitioning information") {
    val originalAnalyzedQuery1 = testRelation3.join(
      testRelation2.join(testRelation0, joinType = Inner, condition = Some(t2id === t0id)),
      joinType = Inner, condition = Some(t2id === t3id)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = testRelation3
      .join(testRelation2, joinType = Inner, condition = Some(t2id === t3id))
      .join(testRelation0, joinType = Inner, condition = Some(t2id === t0id)).analyze

    comparePlans(optimized1, correctAnswer1)

    val originalAnalyzedQuery2 = testRelation0
      .join(testRelation2, joinType = Inner, condition = Some(t0id === t2id))
      .join(testRelation3, joinType = Inner, condition = Some(t3id === t2id)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = testRelation0
      .join(testRelation2
        .join(testRelation3, joinType = Inner, condition = Some(t3id === t2id)),
        joinType = Inner, condition = Some(t0id === t2id)).analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("Parent and children unary operators in logical plans are preserved during rotations") {
    // Check the case when there are preceding operators only
    val originalAnalyzedQuery1 = testRelation1
      .join(testRelation2, joinType = Inner, condition = Some(t1id === t2id))
      .join(testRelation3, joinType = Inner, condition = Some(t2id === t3id))
      .limit('test).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = testRelation1
      .join(testRelation2.join(testRelation3, joinType = Inner, condition = Some(t2id === t3id)),
        joinType = Inner, condition = Some(t1id === t2id)).limit('test).analyze

    comparePlans(optimized1, correctAnswer1)

    // Check the case when there are succeeding operators only
    val originalAnalyzedQuery2 = testRelation2.limit('test)
      .join(testRelation1.limit('test), joinType = Inner, condition = Some(t2id === t1id))
      .join(testRelation3.limit('test), joinType = Inner, condition = Some(t2id === t3id)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = testRelation1.limit('test)
      .join(testRelation2.limit('test)
        .join(testRelation3.limit('test), joinType = Inner, condition = Some(t2id === t3id)),
        joinType = Inner, condition = Some(t2id === t1id)).analyze

    comparePlans(optimized2, correctAnswer2)

    // Check for both preceding and succeeding operators present
    val originalAnalyzedQuery3 = testRelation2.limit('test)
      .join(testRelation1.limit('test), joinType = Inner, condition = Some(t2id === t1id))
      .join(testRelation3.limit('test), joinType = Inner, condition = Some(t2id === t3id))
      .limit('test).subquery('x).analyze

    val optimized3 = Optimize.execute(originalAnalyzedQuery3)

    val correctAnswer3 = testRelation1.limit('test)
      .join(testRelation2.limit('test)
        .join(testRelation3.limit('test), joinType = Inner, condition = Some(t2id === t3id)),
        joinType = Inner, condition = Some(t2id === t1id)).limit('test).subquery('x).analyze

    comparePlans(optimized3, correctAnswer3)
  }

}
