package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{CreateLogicalRelation, SQLContext, sources}
import org.mockito.Mockito
import org.scalatest.FunSuite

// scalastyle:off magic.number
// scalastyle:off multiple.string.literals

class SqlBuilderSuite extends FunSuite with SqlBuilderSuiteBase {

  override val sqlBuilder = new SqlBuilder
  import sqlBuilder._ // scalastyle:ignore

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
    override def schema: StructType = StructType(Seq(
      StructField("c1", StringType),
      StructField("c2", StringType)
    ))
    override def tableName: String = "t1"
  })
  val t2 = CreateLogicalRelation(new BaseRelation with SqlLikeRelation {
    override def sqlContext: SQLContext = _sqlContext
    override def schema: StructType = StructType(Seq(
      StructField("c1", StringType),
      StructField("c2", StringType)
    ))
    override def tableName: String = "t2"
  })

  testLogicalPlan("""SELECT "c1", "c2" FROM "t1"""")(t1)
  testLogicalPlan("""SELECT * FROM "t1"""")(t1.select())
  testLogicalPlan("""SELECT "q"."c1", "q"."c2" FROM "t1" AS "q"""")(t1.subquery('q))
  testLogicalPlan("""SELECT "q"."c1", "q"."c2" FROM "t1" AS "q" LIMIT 100""")(
    t1.subquery('q).limit(100))
  testLogicalPlan("""SELECT * FROM "t1"""")(t1.select().select())
  testLogicalPlan("""SELECT * FROM "t1"""")(t1.select(UnresolvedStar(None)))
  testLogicalPlan("SELECT \"t1\".\"c1\" FROM \"t1\" GROUP BY \"t1\".\"c1\"")({
    val c1 = 'c1.string.withQualifiers("t1" :: Nil)
    val c2 = 'c2.string.withQualifiers("t1" :: Nil)
    t1.select(c1, c2).groupBy(c1)(c1)
  })

  testLogicalPlan("""SELECT "t1"."c1" FROM "t1" GROUP BY "t1"."c1"""")(
    t1.groupBy('c1.string.withQualifiers("t1" :: Nil))('c1.string.withQualifiers("t1" :: Nil))
  )

  testLogicalPlan(
    """
      |SELECT "q"."c1"
      |FROM (SELECT "t1"."c1" FROM "t1") AS "q"
      |GROUP BY "q"."c1"
      |""".stripMargin.replaceAll("(\n|\\s)+", " ").trim)({
    val c1 = 'c1.string.withQualifiers("t1" :: Nil)
    val qc1 = 'c1.string.withQualifiers("q" :: Nil)
    t1.select(c1).subquery('q).select(qc1).groupBy(qc1)(qc1)
  })

  testLogicalPlan(
    """
      |SELECT "q"."c1"
      |FROM (SELECT "t1"."c1" FROM "t1" WHERE ("t1"."c1" = 'string')) AS "q"
      |GROUP BY "q"."c1"
      |""".stripMargin.replaceAll("(\n|\\s)+", " ").trim)({
    val c1 = 'c1.string.withQualifiers("t1" :: Nil)
    val qc1 = 'c1.string.withQualifiers("q" :: Nil)
    t1.select(c1).where(c1 === "string").subquery('q).select(qc1).groupBy(qc1)(qc1)
  })

  testLogicalPlan("""SELECT "c1", "c2" FROM "t1" LIMIT 100""")(t1.limit(100))

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
    s"""SELECT "c1", "c2" FROM "t1" WHERE ("t1"."c1" = 1)"""
  )(t1.where('c1.string.withQualifiers("t1" :: Nil) === 1))

  testLogicalPlan(
    s"""SELECT "c1", "c2" FROM "t1" UNION ALL SELECT "c1", "c2" FROM "t2""""
  )(t1.unionAll(t2))

  testLogicalPlan(
    s"""SELECT "c1", "c2" FROM "t1" EXCEPT SELECT "c1", "c2" FROM "t2""""
  )(Except(t1, t2))

  testLogicalPlan(
    s"""SELECT "c1", "c2" FROM "t1" INTERSECT SELECT "c1", "c2" FROM "t2""""
  )(Intersect(t1, t2))

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
