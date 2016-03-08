package org.apache.spark.sql.sources.sql

import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{LogicalRelation, CreateLogicalRelation}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, sources}
import org.mockito.Mockito
import org.scalatest.FunSuite

// scalastyle:off magic.number
// scalastyle:off multiple.string.literals
class SqlBuilderSuite extends FunSuite with SqlBuilderSuiteBase {

  override val sqlBuilder = new SqlBuilder
  import sqlBuilder._ // scalastyle:ignore

  val simpleTable = TestSqlLikeRelation(None, "t")
  val simpleTableWithNamespace = TestSqlLikeRelation(Some("ns"), "t")

  testBuildSelect("SELECT * FROM \"t\"")(simpleTable, Nil, Nil)
  testBuildSelect("SELECT * FROM \"ns\".\"t\"")(simpleTableWithNamespace, Nil, Nil)
  testBuildSelect("SELECT \"one\" FROM \"t\"")(
    simpleTable, Seq("one"), Nil
  )
  testBuildSelect("SELECT \"one\", \"two\" FROM \"t\"")(
    simpleTable, Seq("one", "two"), Nil
  )
  testBuildSelect("SELECT \"one\", \"two\", \"three\" FROM \"t\"")(
    simpleTable, Seq("one", "two", "three"), Nil
  )

  testBuildSelect("SELECT * FROM \"t\" WHERE \"a\" = 'b'")(
    simpleTable, Nil, Seq(sources.EqualTo("a", "b"))
  )
  testBuildSelect("SELECT \"one\" FROM \"t\" WHERE \"a\" = 'b'")(
    simpleTable, Seq("one"), Seq(sources.EqualTo("a", "b"))
  )
  testBuildSelect("SELECT \"one\" FROM \"t\" WHERE \"a\" = 1")(
    simpleTable, Seq("one"), Seq(sources.EqualTo("a", 1))
  )
  testBuildSelect("SELECT \"one\" FROM \"t\" WHERE \"a\" < 1")(
    simpleTable, Seq("one"), Seq(sources.LessThan("a", 1L))
  )
  testBuildSelect("SELECT \"one\" FROM \"t\" WHERE \"a\" = NULL")(
    simpleTable, Seq("one"), Seq(sources.EqualTo("a", null))
  )

  testBuildSelect(
    "SELECT * FROM \"t\" WHERE \"a\" = 'b' AND \"b\" IS NULL")(
      simpleTable, Nil, Seq(sources.EqualTo("a", "b"), sources.IsNull("b"))
  )
  testBuildSelect(
    "SELECT * FROM \"t\" WHERE \"a\" = 'b' AND (\"b\" IS NULL OR \"c\" IS NOT NULL)")(
       simpleTable, Nil, Seq(sources.EqualTo("a", "b"), sources.Or(sources.IsNull("b"),
          sources.IsNotNull("c")
        ))
      )

  testBuildSelect(
    "SELECT * FROM \"t\" WHERE \"a\" IN (1,2,3,4)")(
      simpleTable, Nil, Seq(sources.In("a", Array(1, 2, 3, 4)))
  )

  testBuildSelect(
    "SELECT * FROM \"t\" WHERE NOT(\"a\" IN (1,2,3,4))")(
      simpleTable, Nil, Seq(sources.Not(sources.In("a", Array(1, 2, 3, 4))))
    )

  testBuildSelect(
    "SELECT * FROM \"t\" WHERE \"a\" LIKE '%b'")(
      simpleTable, Nil, Seq(sources.StringEndsWith("a", "b"))
  )

  testBuildSelect(
    "SELECT * FROM \"t\" WHERE \"a\" LIKE 'b%'")(
    simpleTable, Nil, Seq(sources.StringStartsWith("a", "b"))
  )

  testBuildSelect(
    "SELECT * FROM \"t\" WHERE \"a\" LIKE '%b%'")(
    simpleTable, Nil, Seq(sources.StringContains("a", "b"))
  )

  testExpressionToSql("AVG(1) AS \"PartialAvg\"")(avg(1) as "PartialAvg")
  testExpressionToSql("SUM(1) AS \"PartialSum\"")(sum(1) as "PartialSum")
  testExpressionToSql("COUNT(1) AS \"PartialCount\"")(count(1) as "PartialCount")
  testExpressionToSql("MAX(1) AS \"PartialMax\"")(max(1) as "PartialMax")
  testExpressionToSql("MIN(1) AS \"PartialMin\"")(min(1) as "PartialMin")
  testExpressionToSql("1 IN ()")(Literal(1).in()) /* XXX: Should we allow this case */
  testExpressionToSql("1 IN (\"a\", \"b\", 2, MAX(1))")(Literal(1).in('a, 'b, 2, max(1)))
  testExpressionToSql("1 IN (1, 2, 3)")(InSet(1, Set[Any](1, 2, 3)))
  testExpressionToSql("ltrim('s')")(StringTrimLeft("s"))
  testExpressionToSql("rtrim('s')")(StringTrimRight("s"))
  testExpressionToSql("\"a\" LIKE '%b'")(Like('a, "%b"))

  val _sqlContext = Mockito.mock(classOf[SQLContext])
  val t1 = CreateLogicalRelation(new BaseRelation with SqlLikeRelation {
    override def sqlContext: SQLContext = _sqlContext
    override def schema: StructType = StructType(Seq(
      StructField("c1", StringType),
      StructField("c2", StringType)
    ))
    override def tableName: String = "t1"

  })
  val t1c1 = t1.output.find(_.name == "c1").get
  val t1c2 = t1.output.find(_.name == "c2").get
  val t2 = CreateLogicalRelation(new BaseRelation with SqlLikeRelation {
    override def sqlContext: SQLContext = _sqlContext
    override def schema: StructType = StructType(Seq(
      StructField("c1", StringType),
      StructField("c2", StringType)
    ))
    override def tableName: String = "t2"

  })
  val t2c1 = t2.output.find(_.name == "c1").get
  val t2c2 = t2.output.find(_.name == "c2").get

  testLogicalPlanInternal("""SELECT "c1", "c2" FROM "t1"""")(t1)
  testLogicalPlan("""SELECT "c1", "c2" FROM "t1"""")(t1)

  testLogicalPlanInternal("""SELECT * FROM "t1"""")(t1.select())
  testLogicalPlan("""SELECT "c1", "c2" FROM "t1"""")(t1.select())

  testLogicalPlanInternal("""SELECT "q"."c1", "q"."c2" FROM "t1" AS "q"""")(t1.subquery('q))
  testLogicalPlan("""SELECT "c1", "c2" FROM "t1"""")(t1.subquery('q))

  testLogicalPlanInternal("""SELECT "q"."c1", "q"."c2" FROM "t1" AS "q" LIMIT 1""")(
    t1.subquery('q).limit(1))
  testLogicalPlan("""SELECT "q"."c1", "q"."c2" FROM "t1" AS "q" LIMIT 1""")(
    t1.subquery('q).limit(1))

  /* SelectOperation merges both projects */
  testUnsupportedLogicalPlanInternal(t1.select().select())
  testLogicalPlan("""SELECT "c1", "c2" FROM "t1"""")(
    t1.select().select())

  testLogicalPlanInternal("""SELECT * FROM "t1"""")(t1.select(UnresolvedStar(None)))
  testLogicalPlan("""SELECT * FROM "t1"""")(t1.select(UnresolvedStar(None)))

  testUnsupportedLogicalPlanInternal({
    val c1 = t1.output.find(_.name == "c1").get
    val c2 = t1.output.find(_.name == "c2").get
    t1.select(c1, c2).groupBy(c1)(c1)
  })
  testLogicalPlan(
    """SELECT "__subquery1"."c1" """ +
      """FROM (SELECT "t1"."c1", "t1"."c2" FROM "t1") AS "__subquery1" """ +
      """GROUP BY "__subquery1"."c1""""
  )(t1.select(t1c1, t1c2).groupBy(t1c1)(t1c1))

  testLogicalPlan("""SELECT "t1"."c1", "t1"."c2" FROM "t1" LIMIT 1""")(
    t1.limit(1).subquery('q))

  testLogicalPlan("""SELECT "t1"."c1" FROM "t1" GROUP BY "t1"."c1"""")(
    t1.groupBy(t1c1)(t1c1)
  )

  testLogicalPlan(
    """SELECT "__subquery1"."c1" FROM """ +
      """(SELECT "q"."c1" FROM (SELECT "t1"."c1" FROM "t1") AS "q") AS "__subquery1" """ +
      """GROUP BY "__subquery1"."c1""""
    )({
    val qc1 = t1c1.withQualifiers("q" :: Nil)
    t1.select(t1c1).subquery('q).select(qc1).groupBy(qc1)(qc1)
  })

  testLogicalPlan(
  """SELECT "__subquery2"."c1" FROM """ +
    """(SELECT "q"."c1" FROM """ +
    """(SELECT "__subquery1"."c1" FROM (SELECT "t1"."c1" FROM "t1") AS "__subquery1" """ +
    """WHERE ("__subquery1"."c1" = 'string')) AS "q") AS "__subquery2" """ +
    """GROUP BY "__subquery2"."c1""""
  )({
    val qc1 = t1c1.withQualifiers("q" :: Nil)
    t1.select(t1c1).where(t1c1 === "string").subquery('q).select(qc1).groupBy(qc1)(qc1)
  })

  testLogicalPlan("""SELECT "t1"."c1", "t1"."c2" FROM "t1" LIMIT 1""")(
    t1.limit(1))

  testLogicalPlan(
    """SELECT "t1"."c1", "t2"."c2" FROM "t1" INNER JOIN "t2" ON ("t1"."c1" = "t2"."c2")"""
  )(
    t1.join(t2, Inner,
      Some(
        t1.output.find(_.name == "c1").get.withQualifiers("t1" :: Nil) ===
          t2.output.find(_.name == "c2").get.withQualifiers("t2" :: Nil)
      )
    ).select(t1.output.find(_.name == "c1").get.withQualifiers("t1" :: Nil),
        t2.output.find(_.name == "c2").get.withQualifiers("t2" :: Nil))
  )

  testLogicalPlan(
    """SELECT "t1"."c1", "t2"."c2" FROM "t1" CROSS JOIN "t2""""
  )(
      t1.join(t2, Inner).select(t1.output.find(_.name == "c1").get.withQualifiers("t1" :: Nil),
          t2.output.find(_.name == "c2").get.withQualifiers("t2" :: Nil))
    )

  testLogicalPlan(
    """SELECT "t1"."c1", "t2"."c2" FROM "t1" FULL OUTER JOIN "t2" ON ("t1"."c1" = "t2"."c2")"""
  )(
      t1.join(t2, FullOuter,
        Some(
          t1.output.find(_.name == "c1").get.withQualifiers("t1" :: Nil) ===
            t2.output.find(_.name == "c2").get.withQualifiers("t2" :: Nil)
        )
      ).select(t1.output.find(_.name == "c1").get.withQualifiers("t1" :: Nil),
          t2.output.find(_.name == "c2").get.withQualifiers("t2" :: Nil))
    )

  testLogicalPlan(
    """SELECT "t1"."c1", "t2"."c2" FROM "t1" RIGHT OUTER JOIN "t2" ON ("t1"."c1" = "t2"."c2")"""
  )(
      t1.join(t2, RightOuter,
        Some(
          t1.output.find(_.name == "c1").get.withQualifiers("t1" :: Nil) ===
            t2.output.find(_.name == "c2").get.withQualifiers("t2" :: Nil)
        )
      ).select(t1.output.find(_.name == "c1").get.withQualifiers("t1" :: Nil),
          t2.output.find(_.name == "c2").get.withQualifiers("t2" :: Nil))
    )

  testLogicalPlan(
    """SELECT "t1"."c1", "t2"."c2" FROM "t1" LEFT OUTER JOIN "t2" ON ("t1"."c1" = "t2"."c2")"""
  )(
      t1.join(t2, LeftOuter,
        Some(
          t1.output.find(_.name == "c1").get.withQualifiers("t1" :: Nil) ===
            t2.output.find(_.name == "c2").get.withQualifiers("t2" :: Nil)
        )
      ).select(t1.output.find(_.name == "c1").get.withQualifiers("t1" :: Nil),
          t2.output.find(_.name == "c2").get.withQualifiers("t2" :: Nil))
    )

  testLogicalPlan(
    """SELECT "t1"."c1", "t2"."c2" FROM "t1" LEFT SEMI JOIN "t2" ON ("t1"."c1" = "t2"."c2")"""
  )(
      t1.join(t2, LeftSemi,
        Some(
          t1.output.find(_.name == "c1").get.withQualifiers("t1" :: Nil) ===
            t2.output.find(_.name == "c2").get.withQualifiers("t2" :: Nil)
        )
      ).select(t1.output.find(_.name == "c1").get.withQualifiers("t1" :: Nil),
          t2.output.find(_.name == "c2").get.withQualifiers("t2" :: Nil))
    )

  testLogicalPlan(
    """SELECT "t1"."c1", "t1"."c2" FROM "t1" WHERE ("t1"."c1" = 1)"""
  )(t1.where(t1.output.find(_.name == "c1").get === 1))

  testLogicalPlan(
    """SELECT DISTINCT "t1"."c1", "t1"."c2" FROM "t1""""
  )(Distinct(t1))

  val c1 = t1.output.find(_.name == "c1").get
  testLogicalPlan(
    """SELECT DISTINCT "t1"."c1" FROM "t1" GROUP BY "t1"."c1""""
  )(Distinct(t1.groupBy(c1)(c1)))

  testLogicalPlan(
    """SELECT "c1", "c2" FROM "t1" UNION SELECT "c1", "c2" FROM "t2""""
  )(Distinct(t1.unionAll(t2)))

  testLogicalPlan(
    """SELECT DISTINCT "t1"."c1", "t1"."c2" """ +
      """FROM "t1" UNION ALL SELECT DISTINCT "t2"."c1", "t2"."c2" FROM "t2""""
  )(Distinct(t1).unionAll(Distinct(t2)))

  testLogicalPlan(
    s"""SELECT "c1", "c2" FROM "t1" UNION ALL SELECT "c1", "c2" FROM "t2""""
  )(t1.unionAll(t2))

  testLogicalPlan(
    s"""SELECT "c1", "c2" FROM "t1"
        |UNION ALL
        |SELECT "c1", "c2" FROM "t2"
        |EXCEPT SELECT "c1", "c2" FROM "t1" """
      .stripMargin
  )(t1.unionAll(t2).except(t1))

  testLogicalPlan(
    """SELECT "__subquery1"."c1", "__subquery1"."c2" FROM """ +
    """(SELECT "c1", "c2" FROM "t1" UNION ALL SELECT "c1", "c2" FROM "t2") AS "__subquery1" """ +
    """ORDER BY "__subquery1"."c1" DESC"""
  )(t1.unionAll(t2).orderBy(c1.desc))

  testLogicalPlan(
    s"""SELECT "c1", "c2" FROM "t1" EXCEPT SELECT "c1", "c2" FROM "t2""""
  )(Except(t1, t2))

  testLogicalPlan(
    """SELECT "__subquery1"."c1", "__subquery1"."c2" FROM """ +
      """(SELECT "c1", "c2" FROM "t1" EXCEPT SELECT "c1", "c2" FROM "t2") AS "__subquery1" """ +
      """ORDER BY "__subquery1"."c1" DESC"""
  )(Except(t1, t2).orderBy(c1.desc))

  testLogicalPlan(
    s"""SELECT "c1", "c2" FROM "t1" INTERSECT SELECT "c1", "c2" FROM "t2""""
  )(Intersect(t1, t2))

  testLogicalPlan(
    """SELECT "__subquery1"."c1", "__subquery1"."c2" FROM """ +
      """(SELECT "c1", "c2" FROM "t1" INTERSECT SELECT "c1", "c2" FROM "t2") AS "__subquery1" """ +
      """ORDER BY "__subquery1"."c1" DESC"""
  )(Intersect(t1, t2).orderBy(c1.desc))

  testLogicalPlan(
    s"""
       |  SELECT "c1", "c2" FROM "t1"
       |UNION
       |  SELECT "c1", "c2" FROM "t2"
       |  EXCEPT
       |  SELECT "c1", "c2" FROM "t1"
     """.stripMargin
  )(Distinct(t1.unionAll(t2.except(t1))))

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
