package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.scalatest.FunSuite

trait SqlBuilderSuiteBase {
  self: FunSuite =>

  val sqlBuilder: SqlBuilder
  import sqlBuilder._ // scalastyle:ignore

  def testExpressionToSql(result: String)(expr: Expression): Unit = {
    test(s"expressionToSql: $result | with $expr") {
      assertResult(result)(sqlBuilder.expressionToSql(expr))
    }
  }

  def testBuildSelect[F: ToSql,H: ToSql]
  (result: String)(i1: String, i2: Seq[F], i3: Seq[H]): Unit = {
    test(s"buildSelect: $result | with $i1 $i2 $i3") {
      assertResult(result)(sqlBuilder.buildSelect(i1, i2, i3))
    }
  }

  def testBuildSelect[F: ToSql,H: ToSql,G: ToSql]
  (result: String)(i1: String, i2: Seq[F], i3: Seq[H], i4: Seq[G]): Unit = {
    test(s"buildSelect with group by: $result | with $i1 $i2 $i3") {
      assertResult(result)(sqlBuilder.buildSelect(i1, i2, i3, i4))
    }
  }

  def testLogicalPlan(result: String)(plan: LogicalPlan): Unit = {
    test(s"logical plan: $result | with $plan") {
      assertResult(result)(sqlBuilder.logicalPlanToSql(plan))
    }
  }

  def testUnsupportedLogicalPlan(plan: LogicalPlan): Unit = {
    test(s"invalid logical plan: $plan") {
      intercept[RuntimeException] {
        sqlBuilder.logicalPlanToSql(plan)
      }
    }
  }

}
