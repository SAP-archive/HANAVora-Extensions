package org.apache.spark.sql.catalyst.optimizer

import com.sap.spark.PlanTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.scalatest.FunSuite

class ExtraBooleanSimplificationSuite extends FunSuite with PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    private val MAX_ITERATIONS = 100

    val batches =
      Batch("ExtraBooleanSimplification",
        FixedPoint(MAX_ITERATIONS), ExtraBooleanSimplification) :: Nil

  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("And inside OR simplification left side") {
    val originalAnalyzedQuery = testRelation.where(
      ("a".attr === 1 && "a".attr === 2) || "a".attr === 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(
      ("a".attr === 1 || "a".attr === 3) &&
        ("a".attr === 2 || "a".attr === 3)
    ).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("And inside OR simplification right side") {
    val originalAnalyzedQuery = testRelation.where(
      "a".attr === 3 || ("a".attr === 1 && "a".attr === 2)).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(
      ("a".attr === 1 || "a".attr === 3) &&
        ("a".attr === 2 || "a".attr === 3)
    ).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Or inside AND simplification left side") {
    val originalAnalyzedQuery = testRelation.where(
      ("a".attr === 1 || "a".attr === 2) && "a".attr === 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(
      ("a".attr === 1 || "a".attr === 2) && "a".attr === 3
    ).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Or inside AND simplification right side") {
    val originalAnalyzedQuery = testRelation.where(
      "a".attr === 3 && ("a".attr === 1 || "a".attr === 2)).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(
      "a".attr === 3 && ("a".attr === 1 || "a".attr === 2)
    ).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Ors inside AND simplification") {
    val originalAnalyzedQuery = testRelation.where(
      ("a".attr === 1 || "a".attr === 2) &&
        ("a".attr > 1 || "a".attr > 2)).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(
      ("a".attr === 1 || "a".attr === 2) &&
        ("a".attr > 1 || "a".attr > 2)
    ).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Ands inside OR simplification") {
    val originalAnalyzedQuery = testRelation.where(
      ("a".attr === 1 && "a".attr === 2) ||
        ("a".attr > 1 && "a".attr > 2)).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(
      (("a".attr > 1 || "a".attr === 1) &&
        ("a".attr > 2 || "a".attr === 1)) &&
        (("a".attr > 1 || "a".attr === 2) &&
          ("a".attr > 2 || "a".attr === 2))
    ).analyze

    comparePlans(optimized, correctAnswer)
  }

}
