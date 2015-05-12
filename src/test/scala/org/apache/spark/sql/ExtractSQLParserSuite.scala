package org.apache.spark.sql

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.scalatest.FunSuite

class ExtractSQLParserSuite extends FunSuite with Logging {

  // scalastyle:off magic.number

  val t1 = UnresolvedRelation("T1" :: Nil)
  val parser = new HierarchiesSQLParser

  test("Parse EXTRACT in SELECT") {
    val result = parser("SELECT a, EXTRACT(YEAR FROM a) FROM T1")
    val expected = t1.select('a, Extract("YEAR", 'a).as("c1"))
    assertResult(expected)(result)
  }

  test("Parse EXTRACT in WHERE") {
    val result = parser("SELECT * FROM T1 WHERE EXTRACT(YEAR FROM a) = 2015")
    val expected = t1.where(Extract("YEAR", 'a) === 2015).select(UnresolvedStar(None))
    assertResult(expected)(result)
  }

  test("Parse EXTRACT in GROUP BY") {
    val result = parser("SELECT * FROM T1 GROUP BY EXTRACT(YEAR FROM a)")
    val expected = t1.groupBy(Extract("YEAR", 'a))(UnresolvedStar(None))
    assertResult(expected)(result)
  }

}
