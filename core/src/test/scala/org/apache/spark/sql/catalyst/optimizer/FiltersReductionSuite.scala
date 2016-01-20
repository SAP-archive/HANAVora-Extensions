package org.apache.spark.sql.catalyst.optimizer

import com.sap.spark.PlanTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

// scalastyle:off file.size.limit
class FiltersReductionSuite
  extends FunSuite
  with PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    private val MAX_ITERATIONS = 100

    val batches =
      Batch("FiltersSimplification", FixedPoint(MAX_ITERATIONS), FiltersReduction) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  val testRelation1 = LocalRelation('d.int, 'e.int, 'f.int)

  /**
   * Types handling tests
   */
  test("Reducing redundant greater than filters with Int and Long") {
    val originalAnalyzedQuery = testRelation.where("a".attr > 3 &&
      "a".attr > 21474836470L).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 21474836470L).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Reducing redundant greater than filters with Int and Float") {
    val originalAnalyzedQuery = testRelation.where("a".attr > 3 &&
      "a".attr > 214.9F).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 214.9F).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Reducing redundant greater than filters with Int and Double") {
    val originalAnalyzedQuery = testRelation.where("a".attr > 3 &&
    "a".attr > 214.9).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 214.9).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Reducing redundant greater than filters with Int and Short") {
    val originalAnalyzedQuery = testRelation.where("a".attr > 1 &&
    "a".attr > 2.toShort).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 2.toShort).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Reducing redundant greater than filters with Int and Decimal") {
    val originalAnalyzedQuery = testRelation.where('a.attr > 3 &&
    'a.attr > Decimal("214.9")).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where('a.attr > Decimal("214.9")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Reducing redundant greater than filters with Int and java.math.BigDecimal") {
    val originalAnalyzedQuery = testRelation.where("a".attr > 1 &&
    "a".attr > new java.math.BigDecimal(2)).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > new java.math.BigDecimal(2)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Reducing redundant greater than filters with Int and BigDecimal") {
    val originalAnalyzedQuery = testRelation.where("a".attr > 1 &&
    "a".attr > BigDecimal(2)).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > BigDecimal(2)).analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
    * Greater than AND tests
   */
  test("c1 > 5 && c1 > 3 => c1 > 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 && "a".attr > 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 && c1 > 5 => c1 > 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 && "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 3 && c1 > 5 => c1 > 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 3 && "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test(" c1 > 5 && c1 >= 3 => c1 > 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 && "a".attr >= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 && c1 >= 5 => c1 > 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 && "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 3 && c1 >= 5 => c1 >= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 3 && "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 && c1 >= 3 => c1 >= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 && "a".attr >= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 && c1 >= 5 => c1 >= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 && "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 3 && c1 >= 5 => c1 >= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 3 && "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 && c1 > 3 => c1 >= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 && "a".attr > 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 && c1 > 5 => c1 > 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 && "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 3 && c1 > 5 => c1 > 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 3 && "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
   * Greater than - equals AND tests
   */
  test("c1 > 5 && c1 == 3 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 && "a".attr === 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 && c1 == 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 && "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 3 && c1 == 5 => c1 == 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 3 && "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 && c1 > 3 => c1 == 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 && "a".attr > 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 && c1 > 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 && "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 && c1 > 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 3 && "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 && c1 == 3 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 && "a".attr === 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 && c1 == 5 => c1 == 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 && "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 3 && c1 == 5 => c1 == 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 3 && "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 && c1 >= 3 => c1 == 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 && "a".attr >= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 && c1 >= 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 && "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 && c1 >= 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 3 && "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
   * Less than AND tests
   */
  test("c1 < 3 && c1 < 5 => c1 < 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 3 && "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 && c1 < 5 => c1 < 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 && "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 && c1 < 3 => c1 < 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 && "a".attr < 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 3 && c1 <= 5 => c1 < 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 3 && "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 && c1 <= 5 => c1 < 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 && "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 && c1 <= 3 => c1 <= 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 && "a".attr <= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 3 && c1 <= 5 => c1 <= 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 3 && "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 && c1 <= 5 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 && "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 && c1 <= 3 => c1 <= 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 && "a".attr <= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 3 && c1 < 5 => c1 <= 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 3 && "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 && c1 < 5 => c1 < 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 && "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 && c1 < 3 => c1 < 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 && "a".attr < 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
   * Less than - equals AND tests
   */
  test("c1 < 3 && c1 == 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 3 && "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 && c1 == 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 && "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 && c1 == 3 => c1 == 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 && "a".attr === 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 && c1 < 5 => c == 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 3 && "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 && c1 < 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 && "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 && c1 < 3 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 && "a".attr < 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 3 && c1 == 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 3 && "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 && c1 == 5 => c1 == 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 && "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 && c1 == 3 => c1 == 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 && "a".attr === 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 && c1 <= 5 => c1 == 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 3 && "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 && c1 <= 5 => c1 == 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 && "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 && c1 <= 3 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 && "a".attr <= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
   * Greater than - less than AND filters
   */
  test("c1 > 5 && c1 < 3 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 && "a".attr < 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 && c1 < 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 && "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 3 && c1 < 5 => c1 > 3 && c1 < 5 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 3 && "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 3 && "a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 && c1 <= 3 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 && "a".attr <= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 && c1 <= 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 && "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 3 && c1 <= 5 => c1 > 3 && c1 <= 5 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 3 && "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 3 && "a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 && c1 < 3 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 && "a".attr < 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 && c1 < 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 && "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 3 && c1 < 5 => c1 >= 3 && c1 < 5 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 3 && "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 3 && "a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 && c1 <= 3 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 && "a".attr <= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 && c1 <= 5 => c1 == 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 && "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 3 && c1 <= 5 => c1 >= 3 && c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 3 && "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 3 && "a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 3 && c1 > 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 3 && "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 && c1 > 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 && "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 && c1 > 3 => c1 < 5 && c1 > 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 && "a".attr > 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5 && "a".attr > 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 3 && c1 >= 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 3 && "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 && c1 >= 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 && "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 && c1 >= 3 => c1 < 5 && c1 >= 3 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 && "a".attr >= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5 && "a".attr >= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 3 && c1 > 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 3 && "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 && c1 > 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 && "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 && c1 > 3 => c1 <= 5 && c1 > 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 && "a".attr > 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5 && "a".attr > 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 3 && c1 >= 5 => false") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 3 && "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 && c1 >= 5 => c1 == 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 && "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 && c1 >= 3 => c1 <= 5 && c1 >= 3 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 && "a".attr >= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5 && "a".attr >= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
   * Greater than OR filters
   */
  test("c1 > 3 || c1 > 5 => c1 > 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 3 || "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 || c1 > 5 => c1 > 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 || "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 || c1 > 3 => c1 > 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 || "a".attr > 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 3 || c1 >= 5 => c1 > 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 3 || "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 || c1 >= 5 => c1 >= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 || "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 || c1 >= 3 => c1 >= 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 || "a".attr >= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 3 || c1 >= 5 => c1 >= 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 3 || "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 || c1 >= 5 => c1 >= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 || "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 || c1 >= 3 => c1 >= 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 || "a".attr >= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 3 || c1 > 5 => c1 >= 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 3 || "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 || c1 > 5 => c1 >= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 || "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 || c1 > 3 => c1 > 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 || "a".attr > 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
   * Greater than - equals OR filters
   */
  test("c1 > 3 || c1 == 5 => c1 > 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 3 || "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 || c1 == 5 => c1 >= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 || "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 || c1 == 3 => c1 > 5 || c1 == 3 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 || "a".attr === 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 5 || "a".attr === 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 || c1 > 3 => c1 > 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 || "a".attr > 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 || c1 > 5 => c1 >= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 || "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 || c1 > 5 => c1 == 3 || c1 > 5 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 3 || "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 3 || "a".attr > 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 3 || c1 == 5 => c1 >= 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 3 || "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 || c1 == 5 => c1 >= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 || "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 || c1 == 3 => c1 >= 5 || c1 == 3 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 5 || "a".attr === 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5 || "a".attr === 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 || c1 => 3 => c1 => 3") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 || "a".attr >= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 || c1 => 5 => c1 >= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 || "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 || c1 => 5 => c1 == 3 || c1 => 5 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 3 || "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 3 || "a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
   * Less than OR filters
   */
  test("c1 < 5 || c1 < 3 => c1 < 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 || "a".attr < 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 || c1 < 5 => c1 < 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 || "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 3 || c1 < 5 => c1 < 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 3 || "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 || c1 <= 3 => c1 < 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 || "a".attr <= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 || c1 <= 5 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 || "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 3 || c1 <= 5 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 3 || "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 || c1 <= 3 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 || "a".attr <= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 || c1 <= 5 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 || "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 3 || c1 <= 5 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 3 || "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 3 || c1 < 5 => c1 < 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 3 || "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 || c1 < 5 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 || "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 || c1 < 3 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 || "a".attr < 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
   * Less than - equals OR filters
   */
  test("c1 < 5 || c1 == 3 => c1 < 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 || "a".attr === 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 || c1 == 5 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 5 || "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 3 || c1 == 5 => c1 < 3 || c1 == 5 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr < 3 || "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 3 || "a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 || c1 < 5 => c1 < 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 3 || "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 || c1 < 5 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 || "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 || c1 < 3 => c1 == 5 || c1 < 3 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 || "a".attr < 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 5 || "a".attr < 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 || c1 == 3 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 || "a".attr === 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 || c1 == 5 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 5 || "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 3 || c1 == 5 => c1 <= 3 || c1 == 5 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr <= 3 || "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 3 || "a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 || c1 <= 5 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 3 || "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 || c1 <= 5 => c1 <= 5") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 || "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 5 || c1 <= 3 => c1 == 5 || c1 <= 3 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr === 5 || "a".attr <= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 5 || "a".attr <= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
   * Greater than - Less than OR filters
   */
  test("c1 > 3 || c1 <= 5 => true") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 3 || "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 || c1 <= 5 => true") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 || "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 || c1 <= 3 => c1 > 5 || c1 <= 3 [NOT REDUCED]") {

    val originalAnalyzedQuery = testRelation.where("a".attr > 5 || "a".attr <= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 5 || "a".attr <= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 3 || c1 < 5 => true") {

    val originalAnalyzedQuery = testRelation.where("a".attr >= 3 || "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 || c1 < 5 => true") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr >= 5 || "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 || c1 < 3 => c1 >= 5 || c1 < 3 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr >= 5 || "a".attr < 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5 || "a".attr < 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 3 || c1 <= 5 => true") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr >= 3 || "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 || c1 <= 5 => true") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr >= 5 || "a".attr <= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 >= 5 || c1 <= 3 => c1 >= 5 || c1 <= 3 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr >= 5 || "a".attr <= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 5 || "a".attr <= 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 3 || c1 < 5 => true") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr > 3 || "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 || c1 < 5 => c1 > 5 || c1 < 5 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr > 5 || "a".attr < 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 5 || "a".attr < 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 > 5 || c1 < 3 => c1 > 5 || c1 < 3 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr > 5 || "a".attr < 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr > 5 || "a".attr < 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 || c1 >= 3 => true") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr < 5 || "a".attr >= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 || c1 >= 5 => true") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr < 5 || "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 3 || c1 >= 5 => c1 < 3 || c1 >= 5 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr < 3 || "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 3 || "a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 || c1 > 3 => true") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr <= 5 || "a".attr > 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 || c1 > 5 => true") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr <= 5 || "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 3 || c1 > 5 => c1 <= 3 || c1 > 5 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr <= 3 || "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 3 || "a".attr > 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 || c1 >= 3 => true") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr <= 5 || "a".attr >= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 5 || c1 >= 5 => true") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr <= 5 || "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 <= 3 || c1 >= 5 => c1 <= 3 || c1 >= 5 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr <= 3 || "a".attr >= 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr <= 3 || "a".attr >= 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 || c1 > 3 => true") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr < 5 || "a".attr > 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(true).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 5 || c1 > 5 => c1 < 5 || c1 > 5 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr < 5 || "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 5 || "a".attr > 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 3 || c1 > 5 => c1 < 3 || c1 > 5 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr < 3 || "a".attr > 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 3 || "a".attr > 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
   * Varia
   */
  test("c1 < 3 && c2 > 3 => c1 < 3 && c2 > 3 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr < 3 && "b".attr > 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 3 && "b".attr > 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 < 3 || c2 > 3 => c1 < 3 || c2 > 3 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr < 3 || "b".attr > 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr < 3 || "b".attr > 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 && c1 == 5 => false") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr === 3 && "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where(false).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 || c1 == 5 => c1 == 3 || c1 == 5 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr === 3 || "a".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 3 || "a".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 && c1 == 3 => c1 == 3") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr === 3 && "a".attr === 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 3).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 && c2 == 5 => c1 == 3 && c2 == 5 [NOT REDUCED]") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr === 3 && "b".attr === 5).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 3 && "b".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 && c2 == 5 && c1 >= 3 => c1 == 3 && c2 == 5") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr === 3 && "b".attr === 5 && "a".attr >= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr === 3 && "b".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("c1 == 3 || c2 == 5 || c1 >= 3 => c1 >= 3 || c2 == 5") {

    val originalAnalyzedQuery =
      testRelation.where("a".attr === 3 || "b".attr === 5 || "a".attr >= 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer = testRelation.where("a".attr >= 3 || "b".attr === 5).analyze

    comparePlans(optimized, correctAnswer)
  }
}
