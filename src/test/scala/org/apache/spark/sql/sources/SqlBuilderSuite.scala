package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{CreateLogicalRelation, SQLContext, sources}
import org.mockito.Mockito
import org.scalatest.FunSuite

// scalastyle:off magic.number
// scalastyle:off multiple.string.literals

class SqlBuilderSuite extends FunSuite {

  val sqlBuilder = new SqlBuilder
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

  testBuildSelect[String, String]("SELECT * FROM \"table\"")("table", Nil, Nil)
  testBuildSelect[String, String]("SELECT \"one\" FROM \"table\"")(
    "table", Seq("one"), Nil
  )
  testBuildSelect[String, String]("SELECT \"one\", \"two\" FROM \"table\"")(
    "table", Seq("one", "two"), Nil
  )
  testBuildSelect[String, String]("SELECT \"one\", \"two\", \"three\" FROM \"table\"")(
    "table", Seq("one", "two", "three"), Nil
  )

  testBuildSelect[String, sources.Filter]("SELECT * FROM \"table\" WHERE \"a\" = 'b'")(
    "table", Nil, Seq(sources.EqualTo("a", "b"))
  )
  testBuildSelect[String, sources.Filter]("SELECT \"one\" FROM \"table\" WHERE \"a\" = 'b'")(
    "table", Seq("one"), Seq(sources.EqualTo("a", "b"))
  )
  testBuildSelect[String, sources.Filter]("SELECT \"one\" FROM \"table\" WHERE \"a\" = 1")(
    "table", Seq("one"), Seq(sources.EqualTo("a", 1))
  )
  testBuildSelect[String, sources.Filter]("SELECT \"one\" FROM \"table\" WHERE \"a\" < 1")(
    "table", Seq("one"), Seq(sources.LessThan("a", 1L))
  )
  testBuildSelect[String, sources.Filter]("SELECT \"one\" FROM \"table\" WHERE \"a\" = NULL")(
    "table", Seq("one"), Seq(sources.EqualTo("a", null))
  )

  testBuildSelect[String, sources.Filter](
    "SELECT * FROM \"table\" WHERE \"a\" = 'b' AND \"b\" IS NULL")(
        "table", Nil, Seq(sources.EqualTo("a", "b"), sources.IsNull("b"))
  )
  testBuildSelect[String, sources.Filter](
    "SELECT * FROM \"table\" WHERE \"a\" = 'b' AND (\"b\" IS NULL OR \"c\" IS NOT NULL)")(
        "table", Nil, Seq(sources.EqualTo("a", "b"), sources.Or(sources.IsNull("b"),
          sources.IsNotNull("c")
        ))
      )

  testBuildSelect[String, sources.Filter](
    "SELECT * FROM \"table\" WHERE \"a\" IN (1,2,3,4)")(
    "table", Nil, Seq(sources.In("a", Array(1, 2, 3, 4)))
  )

  testBuildSelect[String, sources.Filter](
    "SELECT * FROM \"table\" WHERE NOT(\"a\" IN (1,2,3,4))")(
      "table", Nil, Seq(sources.Not(sources.In("a", Array(1, 2, 3, 4))))
    )

  testBuildSelect[Expression, Expression](
    "SELECT SUBSTRING(\"a\", 0, 1) AS \"aa\", \"b\" FROM \"table\" WHERE (\"a\" = 'a')"
  )(
      "table",
      Seq('a.string.substring(0, 1).as("aa"), 'b.int),
      Seq('a.string === "a")
    )

  testBuildSelect[Expression, Expression](
    """SELECT SUBSTRING("a", 0, 1) AS "aa", "b"
      |FROM "table"
      |WHERE ("c" = SUBSTRING("a", 0, 2))""".stripMargin
        .replace("\n", " ").replaceAll("\\s+", " ")
  )(
      "table",
      Seq('a.string.substring(0, 1).as("aa"), 'b.int),
      Seq('c.string === 'a.string.substring(0, 2))
    )

  testBuildSelect[Expression, Expression]("SELECT COUNT(1) AS \"PartialCount\" FROM \"table\"")(
    "table",
    Seq(count(1).as("PartialCount")),
    Nil
  )

  testBuildSelect[Expression,Expression,Expression](
    "SELECT \"a\", COUNT(1) FROM \"table\" GROUP BY \"a\""
  )(
      "table",
      Seq('a.string, count(1)),
      Nil,
      Seq('a.string)
    )

  testExpressionToSql("AVG(1) AS \"PartialAvg\"")(avg(1) as "PartialAvg")
  testExpressionToSql("SUM(1) AS \"PartialSum\"")(sum(1) as "PartialSum")
  testExpressionToSql("COUNT(1) AS \"PartialCount\"")(count(1) as "PartialCount")
  testExpressionToSql("MAX(1) AS \"PartialMax\"")(max(1) as "PartialMax")
  testExpressionToSql("MIN(1) AS \"PartialMin\"")(min(1) as "PartialMin")

  val _sqlContext = Mockito.mock(classOf[SQLContext])
  val t1 = CreateLogicalRelation(new BaseRelation with SqlLikeRelation {
    override def sqlContext: SQLContext = _sqlContext
    override def schema: StructType = StructType(Seq())
    override def tableName: String = "t1"
  })
  val t2 = CreateLogicalRelation(new BaseRelation with SqlLikeRelation {
    override def sqlContext: SQLContext = _sqlContext
    override def schema: StructType = StructType(Seq())
    override def tableName: String = "t2"
  })

  testLogicalPlan("SELECT * FROM \"t1\"")(t1)
  testLogicalPlan("SELECT * FROM \"t1\"")(t1.select())

  testLogicalPlan("""SELECT "t1"."c1" FROM "t1" GROUP BY "t1"."c1"""")(
    t1.groupBy('c1.string.withQualifiers("t1" :: Nil))('c1.string.withQualifiers("t1" :: Nil))
  )

  testLogicalPlan("""SELECT * FROM "t1" LIMIT 100""")(t1.limit(100))

  testLogicalPlan(
    s"""SELECT "t1"."c1", "t2"."c2" FROM "t1" INNER JOIN "t2" ON ("t1"."c1" = "t2"."c2")"""
  )(
    t1.join(t2, Inner,
      Some('c1.string.withQualifiers("t1" :: Nil) === 'c2.string.withQualifiers("t2" :: Nil))
    ).select('c1.string.withQualifiers("t1" :: Nil), 'c2.string.withQualifiers("t2" :: Nil))
  )

  testLogicalPlan(
    s"""SELECT "t1"."c1", "t2"."c2" FROM "t1" INNER JOIN "t2""""
  )(
      t1.join(t2, Inner)
        .select('c1.string.withQualifiers("t1" :: Nil), 'c2.string.withQualifiers("t2" :: Nil))
    )

  testLogicalPlan(
    s"""SELECT "t1"."c1", "t2"."c2" FROM "t1" FULL OUTER JOIN "t2" ON ("t1"."c1" = "t2"."c2")"""
  )(
      t1.join(t2, FullOuter,
        Some('c1.string.withQualifiers("t1" :: Nil) === 'c2.string.withQualifiers("t2" :: Nil))
      ).select('c1.string.withQualifiers("t1" :: Nil), 'c2.string.withQualifiers("t2" :: Nil))
    )

  testLogicalPlan(
    s"""SELECT "t1"."c1", "t2"."c2" FROM "t1" RIGHT OUTER JOIN "t2" ON ("t1"."c1" = "t2"."c2")"""
  )(
      t1.join(t2, RightOuter,
        Some('c1.string.withQualifiers("t1" :: Nil) === 'c2.string.withQualifiers("t2" :: Nil))
      ).select('c1.string.withQualifiers("t1" :: Nil), 'c2.string.withQualifiers("t2" :: Nil))
    )

  testLogicalPlan(
    s"""SELECT "t1"."c1", "t2"."c2" FROM "t1" LEFT OUTER JOIN "t2" ON ("t1"."c1" = "t2"."c2")"""
  )(
      t1.join(t2, LeftOuter,
        Some('c1.string.withQualifiers("t1" :: Nil) === 'c2.string.withQualifiers("t2" :: Nil))
      ).select('c1.string.withQualifiers("t1" :: Nil), 'c2.string.withQualifiers("t2" :: Nil))
    )

  testLogicalPlan(
    s"""SELECT "t1"."c1", "t2"."c2" FROM "t1" LEFT SEMI JOIN "t2" ON ("t1"."c1" = "t2"."c2")"""
  )(
      t1.join(t2, LeftSemi,
        Some('c1.string.withQualifiers("t1" :: Nil) === 'c2.string.withQualifiers("t2" :: Nil))
      ).select('c1.string.withQualifiers("t1" :: Nil), 'c2.string.withQualifiers("t2" :: Nil))
    )

  testLogicalPlan(
    s"""SELECT * FROM "t1" WHERE ("t1"."c1" = 1)"""
  )(t1.where('c1.string.withQualifiers("t1" :: Nil) === 1))

  case object UnsupportedLogicalPlan extends LeafNode {
    override def output: Seq[Attribute] = Seq()
  }
  testUnsupportedLogicalPlan(UnsupportedLogicalPlan)

  /* LogicalRelations must be SqlLikeRelations */
  testUnsupportedLogicalPlan(LogicalRelation(new BaseRelation {
    override def sqlContext: SQLContext = _sqlContext
    override def schema: StructType = StructType(Nil)
  }))

}
