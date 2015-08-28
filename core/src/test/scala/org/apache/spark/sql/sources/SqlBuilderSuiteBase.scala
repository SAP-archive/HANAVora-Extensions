package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.scalatest.FunSuite

trait SqlBuilderSuiteBase {
  self: FunSuite =>

  val sqlBuilder: SqlBuilder
  import sqlBuilder._ // scalastyle:ignore

  def testExpressionToSql(sql: String)(expr: Expression): Unit = {
    val cleanSql = cleanUpSql(sql)
    test(s"expressionToSql: $cleanSql | with $expr") {
      assertResult(cleanSql)(sqlBuilder.expressionToSql(expr))
    }
  }

  def testBuildSelect[F: ToSql,H: ToSql]
  (sql: String)(i1: SqlLikeRelation, i2: Seq[F], i3: Seq[H]): Unit = {
    val cleanSql = cleanUpSql(sql)
    test(s"buildSelect: $cleanSql | with $i1 $i2 $i3") {
      assertResult(cleanSql)(sqlBuilder.buildSelect(i1, i2, i3))
    }
  }

  def testBuildSelect[F: ToSql,H: ToSql,G: ToSql]
  (sql: String)(i1: SqlLikeRelation, i2: Seq[F], i3: Seq[H], i4: Seq[G]): Unit = {
    val cleanSql = cleanUpSql(sql)
    test(s"buildSelect with group by: $cleanSql | with $i1 $i2 $i3") {
      assertResult(cleanSql)(sqlBuilder.buildSelect(i1, i2, i3, i4))
    }
  }

  def testLogicalPlan(sql: String)(plan: LogicalPlan): Unit = {
    val cleanSql = cleanUpSql(sql)
    test(s"logical plan: $cleanSql | with $plan") {
      assertResult(cleanSql)(sqlBuilder.logicalPlanToSql(plan))
    }
  }

  def testLogicalPlanInternal(sql: String)(plan: LogicalPlan): Unit = {
    val cleanSql = cleanUpSql(sql)
    test(s"logical plan (internal): $cleanSql | with $plan") {
      assertResult(cleanSql)(sqlBuilder.internalLogicalPlanToSql(plan, noProject = true))
    }
  }

  def testUnsupportedLogicalPlan(plan: LogicalPlan): Unit = {
    test(s"invalid logical plan: $plan") {
      intercept[RuntimeException] {
        sqlBuilder.logicalPlanToSql(plan)
      }
    }
  }

  private def cleanUpSql(q: String): String =
    q.replaceAll("\\s+", " ").trim

  def testUnsupportedLogicalPlanInternal(plan: LogicalPlan): Unit = {
    test(s"invalid logical plan (internal): $plan") {
      intercept[RuntimeException] {
        sqlBuilder.internalLogicalPlanToSql(plan)
      }
    }
  }

}
