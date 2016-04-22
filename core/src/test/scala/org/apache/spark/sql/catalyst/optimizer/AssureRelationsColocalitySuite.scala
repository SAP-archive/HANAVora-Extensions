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
  val t0 = new LogicalRelation(new BaseRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id0", IntegerType)))
  })
  val t1 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id1", IntegerType)))
    override def partitioningFunctionColumns: Option[Set[String]] = None
    override def partitioningFunctionName: Option[String] = None
  })
  val t2 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id2", IntegerType)))
    override def partitioningFunctionColumns: Option[Set[String]] = Some(Set("id2"))
    override def partitioningFunctionName: Option[String] = Some("F1")
  })
  val t3 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id3", IntegerType)))
    override def partitioningFunctionColumns: Option[Set[String]] = Some(Set("id3"))
    override def partitioningFunctionName: Option[String] = Some("F1")
  })
  val t4 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id4", IntegerType)))
    override def partitioningFunctionColumns: Option[Set[String]] = None
    override def partitioningFunctionName: Option[String] = None
  })
  val t5 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id5", IntegerType)))
    override def partitioningFunctionColumns: Option[Set[String]] = Some(Set("id5"))
    override def partitioningFunctionName: Option[String] = Some("F2")
  })
  val t6 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id6", IntegerType)))
    override def partitioningFunctionColumns: Option[Set[String]] = Some(Set("id6"))
    override def partitioningFunctionName: Option[String] = Some("F2")
  })

  // Join attributes
  val id0 = t0.output.find(_.name == "id0").get
  val id1 = t1.output.find(_.name == "id1").get
  val id2 = t2.output.find(_.name == "id2").get
  val id3 = t3.output.find(_.name == "id3").get
  val id4 = t4.output.find(_.name == "id4").get
  val id5 = t5.output.find(_.name == "id5").get
  val id6 = t6.output.find(_.name == "id6").get

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
    val originalAnalyzedQuery1 = t1
      .join(t2, joinType = Inner, condition = Some(id1 === id2))
      .join(t3, joinType = Inner, condition = Some(id2 === id3)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t1.join(t2.join(t3, joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id1 === id2)).analyze

    comparePlans(optimized1, correctAnswer1)

    // Phase 2 - Inner joins
    val originalAnalyzedQuery2 = t2.join(t1, joinType = Inner, condition = Some(id2 === id1))
      .join(t3, joinType = Inner, condition = Some(id2 === id3)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t1.join(t2.join(t3, joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id2 === id1)).analyze

    comparePlans(optimized2, correctAnswer2)

    // Phase 2 - Outer joins
    val originalAnalyzedQuery3 = t2.join(t1, joinType = LeftOuter, condition = Some(id2 === id1))
      .join(t3, joinType = Inner, condition = Some(id2 === id3)).analyze

    val optimized3 = Optimize.execute(originalAnalyzedQuery3)

    // The plan should remain unchanged due to the left outer join
    comparePlans(optimized3, originalAnalyzedQuery3)

    val originalAnalyzedQuery4 = t2.join(t1, joinType = RightOuter, condition = Some(id2 === id1))
      .join(t3, joinType = Inner, condition = Some(id2 === id3)).analyze

    val optimized4 = Optimize.execute(originalAnalyzedQuery4)

    // The plan should remain unchanged due to the right outer join
    comparePlans(optimized4, originalAnalyzedQuery4)

    val originalAnalyzedQuery5 = t2.join(t1, joinType = FullOuter, condition = Some(id2 === id1))
      .join(t3, joinType = RightOuter, condition = Some(id2 === id3)).analyze

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
    val originalAnalyzedQuery1 = t3.join(t2
      .join(t1, joinType = Inner, condition = Some(id2 === id1)),
      joinType = Inner, condition = Some(id3 === id2)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t3.join(t2, joinType = Inner, condition = Some(id3 === id2))
      .join(t1, joinType = Inner, condition = Some(id2 === id1)).analyze

    comparePlans(optimized1, correctAnswer1)

    // Phase 2 - Inner joins
    val originalAnalyzedQuery2 = t3.join(t1
      .join(t2, joinType = Inner, condition = Some(id1 === id2)),
      joinType = Inner, condition = Some(id3 === id2)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t3.join(t2, joinType = Inner, condition = Some(id3 === id2))
      .join(t1, joinType = Inner, condition = Some(id1 === id2)).analyze

    comparePlans(optimized2, correctAnswer2)

    // Phase 2 - Outer joins
    val originalAnalyzedQuery3 = t3.join(
      t1.join(t2, joinType = FullOuter, condition = Some(id1 === id2)),
      joinType = Inner, condition = Some(id3 === id2)).analyze

    val optimized3 = Optimize.execute(originalAnalyzedQuery3)

    // The plan should remain unchanged due to the full outer join
    comparePlans(optimized3, originalAnalyzedQuery3)

    val originalAnalyzedQuery4 = t3.join(t1
      .join(t2, joinType = LeftOuter, condition = Some(id1 === id2)),
      joinType = Inner, condition = Some(id3 === id2)).analyze

    val optimized4 = Optimize.execute(originalAnalyzedQuery4)

    // The plan should remain unchanged due to the left outer join
    comparePlans(optimized4, originalAnalyzedQuery4)

    val originalAnalyzedQuery5 = t3.join(t1
      .join(t2, joinType = RightOuter, condition = Some(id1 === id2)),
      joinType = Inner, condition = Some(id3 === id2)).analyze

    val optimized5 = Optimize.execute(originalAnalyzedQuery5)

    // The plan should remain unchanged due to the right outer join
    comparePlans(optimized5, originalAnalyzedQuery5)
  }

  test("The join condition is properly altered in case when the upper join condition " +
    "involves columns from a non-partitioned table") {
    val originalAnalyzedQuery1 = t3.join(t2
      .join(t1, joinType = Inner, condition = Some(id2 === id1)),
      joinType = Inner, condition = Some(id1 === id3)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t3.join(t2, joinType = Inner, condition = Some(id2 === id3))
      .join(t1, joinType = Inner, condition = Some(id2 === id1)).analyze

    comparePlans(optimized1, correctAnswer1)

    val originalAnalyzedQuery2 = t1.join(t2, joinType = Inner, condition = Some(id1 === id2))
      .join(t3, joinType = Inner, condition = Some(id3 === id1)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t1.join(t2.join(t3, joinType = Inner, condition = Some(id3 === id2)),
      joinType = Inner, condition = Some(id1 === id2)).analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("A proper rotation is applied even in case when a join involves tables " +
    "which do not provide partitioning information") {
    val originalAnalyzedQuery1 = t3.join(t2
      .join(t0, joinType = Inner, condition = Some(id2 === id0)),
      joinType = Inner, condition = Some(id2 === id3)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t3.join(t2, joinType = Inner, condition = Some(id2 === id3))
      .join(t0, joinType = Inner, condition = Some(id2 === id0)).analyze

    comparePlans(optimized1, correctAnswer1)

    val originalAnalyzedQuery2 = t0.join(t2, joinType = Inner, condition = Some(id0 === id2))
      .join(t3, joinType = Inner, condition = Some(id3 === id2)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t0.join(t2.join(t3, joinType = Inner, condition = Some(id3 === id2)),
      joinType = Inner, condition = Some(id0 === id2)).analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("Parent and children unary operators in logical plans are preserved during rotations") {
    // Check the case when there are preceding operators only
    val originalAnalyzedQuery1 = t1.join(t2, joinType = Inner, condition = Some(id1 === id2))
      .join(t3, joinType = Inner, condition = Some(id2 === id3))
      .limit('test).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t1.join(t2.join(t3, joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id1 === id2)).limit('test).analyze

    comparePlans(optimized1, correctAnswer1)

    // Check the case when there are succeeding operators only
    val originalAnalyzedQuery2 = t2.limit('test)
      .join(t1.limit('test), joinType = Inner, condition = Some(id2 === id1))
      .join(t3.limit('test), joinType = Inner, condition = Some(id2 === id3)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t1.limit('test).join(t2.limit('test)
      .join(t3.limit('test), joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id2 === id1)).analyze

    comparePlans(optimized2, correctAnswer2)

    // Check for both preceding and succeeding operators present
    val originalAnalyzedQuery3 = t2.limit('test)
      .join(t1.limit('test), joinType = Inner, condition = Some(id2 === id1))
      .join(t3.limit('test), joinType = Inner, condition = Some(id2 === id3))
      .limit('test).subquery('x).analyze

    val optimized3 = Optimize.execute(originalAnalyzedQuery3)

    val correctAnswer3 = t1.limit('test).join(t2.limit('test)
      .join(t3.limit('test), joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id2 === id1)).limit('test).subquery('x).analyze

    comparePlans(optimized3, correctAnswer3)
  }

  test("Parent and children binary operators in logical plans are preserved during rotations") {
    val originalAnalyzedQuery1 = t1.join(t2, joinType = Inner, condition = Some(id1 === id2))
      .join(t3, joinType = Inner, condition = Some(id2 === id3))
      .intersect(t4.join(t5, joinType = Inner, condition = Some(id4 === id5))
        .join(t6, joinType = Inner, condition = Some(id5 === id6))).limit('test).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t1.join(t2.join(t3, joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id1 === id2))
      .intersect(t4.join(t5.join(t6, joinType = Inner, condition = Some(id5 === id6)),
        joinType = Inner, condition = Some(id4 === id5))).limit('test).analyze

    comparePlans(optimized1, correctAnswer1)

    val originalAnalyzedQuery2 = t1.join(t2, joinType = Inner, condition = Some(id1 === id2))
      .join(t3, joinType = Inner, condition = Some(id2 === id3))
      .intersect(t4.join(t5, joinType = Inner, condition = Some(id4 === id5))
        .join(t6, joinType = Inner, condition = Some(id5 === id6))).limit('test)
      .unionAll(t0.join(t2, joinType = Inner, condition = Some(id0 === id2))
        .join(t3, joinType = Inner, condition = Some(id3 === id2)).analyze).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t1.join(t2.join(t3, joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id1 === id2))
      .intersect(t4.join(t5.join(t6, joinType = Inner, condition = Some(id5 === id6)),
        joinType = Inner, condition = Some(id4 === id5))).limit('test)
      .unionAll(t0.join(t2.join(t3, joinType = Inner, condition = Some(id3 === id2)),
        joinType = Inner, condition = Some(id0 === id2))).analyze

    comparePlans(optimized2, correctAnswer2)
  }

}
