package org.apache.spark.sql.catalyst.optimizer

import com.sap.spark.PlanTest
import org.apache.spark.MockitoSparkContext
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.{DataFrame, SapSQLContext}
import org.scalatest.FunSuite

class OptimizerWithMockedSparkContextSuite extends FunSuite with PlanTest with MockitoSparkContext {

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

  lazy val sqlc = new SapSQLContext(sc)

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlc.registerDataFrameAsTable(DataFrame(sqlc, testRelationX), "x")
    sqlc.registerDataFrameAsTable(DataFrame(sqlc, testRelationY), "y")
  }

  test("And inside OR simplification with Rand function") {

    val query =
      sqlc.parseSql("SELECT * FROM x, y WHERE (x.a > 2 AND y.d < 2) OR (x.c > 3 AND RAND(2))")

    val originalAnalyzedQuery = sqlc.analyzer.execute(query)

    val optimized = Optimize.execute(originalAnalyzedQuery)

    comparePlans(optimized, originalAnalyzedQuery)
  }

}
