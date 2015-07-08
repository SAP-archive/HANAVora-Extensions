package org.apache.spark.sql

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest

class ExtractSQLParserSuite extends PlanTest with Logging {

  // scalastyle:off magic.number

  val t1 = UnresolvedRelation("T1" :: Nil)
  val parser = new VelocitySqlParser

  test("Parse EXTRACT in SELECT") {
    val result = parser.parse("SELECT a, EXTRACT(YEAR FROM a) FROM T1")
    val expected = t1.select('a, Extract("YEAR", 'a).as("c1"))
    comparePlans(expected, result)
  }

  test("Parse EXTRACT in WHERE") {
    val result = parser.parse("SELECT * FROM T1 WHERE EXTRACT(YEAR FROM a) = 2015")
    val expected = t1.where(Extract("YEAR", 'a) === 2015).select(UnresolvedStar(None))
    comparePlans(expected, result)
  }

  test("Parse EXTRACT in GROUP BY") {
    val result = parser.parse("SELECT * FROM T1 GROUP BY EXTRACT(YEAR FROM a)")
    val expected = t1.groupBy(Extract("YEAR", 'a))(UnresolvedStar(None))
    comparePlans(expected, result)
  }

}
