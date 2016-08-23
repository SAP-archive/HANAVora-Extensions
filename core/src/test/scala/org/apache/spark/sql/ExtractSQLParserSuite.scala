package org.apache.spark.sql

import com.sap.spark.PlanTest
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.parser.SapParserDialect
import org.scalatest.FunSuite

class ExtractSQLParserSuite extends FunSuite with PlanTest with Logging {

  // scalastyle:off magic.number

  val t1 = UnresolvedRelation(TableIdentifier("T1"))
  val parser = new SapParserDialect

  test("Parse EXTRACT in SELECT") {
    val result = parser.parse("SELECT a, EXTRACT(YEAR FROM a) FROM T1")
    val expected = t1.select(AliasUnresolver('a, Year('a)): _*)
    comparePlans(expected, result)
  }

  test("Parse EXTRACT in WHERE") {
    val result = parser.parse("SELECT 1 FROM T1 WHERE EXTRACT(MONTH FROM a) = 2015")
    val expected = t1.where(Month('a) === 2015).select(AliasUnresolver(1): _*)
    comparePlans(expected, result)
  }

  test("Parse EXTRACT in GROUP BY") {
    val result = parser.parse("SELECT 1 FROM T1 GROUP BY EXTRACT(DAY FROM a)")
    val expected = t1.groupBy(DayOfMonth('a))(AliasUnresolver(1): _*)
    comparePlans(expected, result)
  }

}
