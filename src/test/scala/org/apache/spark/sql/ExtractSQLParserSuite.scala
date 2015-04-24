package org.apache.spark.sql

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Extract, Alias, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.FunSuite

class ExtractSQLParserSuite extends FunSuite with Logging {

  // scalastyle:off magic.number

  test("Extract in a Project") {
    val parser = new HierarchiesSQLParser

    val result = parser( """SELECT a, EXTRACT(YEAR FROM a) FROM T1""".stripMargin)

    val expected = Project(
      UnresolvedAttribute("a") ::
        Alias(Extract(Literal("YEAR", StringType), UnresolvedAttribute("a")), "c1")() ::
        Nil,
      UnresolvedRelation("T1" :: Nil))

    assertResult(expected)(result)
  }

  test("Extract in a Filter") {
    val parser = new HierarchiesSQLParser

    val result = parser( """SELECT * FROM T1 WHERE EXTRACT(YEAR FROM a) = 2015""".stripMargin)

    val expected = Project(
      UnresolvedStar(None) :: Nil,
      Filter(
        EqualTo(
          Extract(
            Literal("YEAR", StringType),
            UnresolvedAttribute("a")
          ),
          Literal(2015, IntegerType)
        ), UnresolvedRelation("T1" :: Nil)))

    assertResult(expected)(result)
  }

  test("Extract in a GroupBy") {
    val parser = new HierarchiesSQLParser

    val result = parser( """SELECT * FROM T1 GROUP BY EXTRACT(YEAR FROM a)""".stripMargin)

    val expected = Aggregate(Extract(
      Literal("YEAR", StringType),
      UnresolvedAttribute("a")
    ) :: Nil,
      UnresolvedStar(None) :: Nil,
      UnresolvedRelation("T1" :: Nil)
    )

    assertResult(expected)(result)
  }
}
