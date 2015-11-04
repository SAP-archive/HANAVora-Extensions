package org.apache.spark.sql.catalyst.optimizer

import com.sap.spark.PlanTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.scalatest.FunSuite

// scalastyle:off magic.number
class RedundantDownPushableFiltersSuite extends FunSuite with PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    private val MAX_ITERATIONS = 1

    val batches =
      Batch("ExtraBooleanSimplification",
        FixedPoint(MAX_ITERATIONS), RedundantDownPushableFilters) :: Nil

  }

  val testRelationX = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelationY = LocalRelation('d.int, 'e.int, 'f.int)

  val x = testRelationX.subquery('x)
  val y = testRelationY.subquery('y)

  test("And inside OR simplification left side") {

    val originalAnalyzedQuery = x.join(y)
      .where(("x.a".attr === 1 && "y.d".attr === 2) || "x.c".attr === 3).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer =
      x.join(y).where((("x.a".attr === 1 && "y.d".attr === 2) || "x.c".attr === 3) &&
        ("x.a".attr === 1 || "x.c".attr === 3)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("And inside OR simplification both sides") {

    val originalAnalyzedQuery = x.join(y).
      where(("x.a".attr === 1 && "y.d".attr === 2) || ("x.c".attr === 3 && "y.e".attr === 4)).
      analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer =
      x.join(y).where((("x.a".attr === 1 && "y.d".attr === 2)
        || ("x.c".attr === 3 && "y.e".attr === 4))
        && (("x.a".attr === 1 || "x.c".attr === 3)
        && ("y.d".attr === 2 || "y.e".attr === 4))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Upper AND with an OR nested expression") {

    val originalAnalyzedQuery = x.join(y).
      where(("x.b".attr > 0)
        && (("x.a".attr === 1 && "y.d".attr === 2) || ("x.c".attr === 3 && "y.e".attr === 4))).
      analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer =
      x.join(y).where(("x.b".attr > 0)
        && (("x.a".attr === 1 && "y.d".attr === 2) || ("x.c".attr === 3 && "y.e".attr === 4))
        && (("x.a".attr === 1 || "x.c".attr === 3)
        && ("y.d".attr === 2 || "y.e".attr === 4))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("OR expression with nested OR expressions") {

    val originalAnalyzedQuery = x.join(y).
      where(("x.a".attr === 1 && ("y.d".attr === 2 || "x.b".attr === 2))
        || ("y.e".attr === 3)).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer =
      x.join(y).where(("x.a".attr === 1 && ("y.d".attr === 2 || "x.b".attr === 2))
        || ("y.e".attr === 3)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Expressions without attribute references") {

    val originalAnalyzedQuery = x.join(y).
      where(sum(2) || (sum(3) && sum(4))).analyze

    val optimized = Optimize.execute(originalAnalyzedQuery)

    val correctAnswer =
      x.join(y).
        where((sum(2) || (sum(3) && sum(4))) && ((sum(2) || sum(3)) && (sum(2) || sum(4)))).analyze

    comparePlans(optimized, correctAnswer)
  }

}
