package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.analysis.ResolvedStar
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{CreateLogicalRelation, SQLContext, sources}
import org.mockito.Mockito
import org.scalatest.FunSuite

// scalastyle:off magic.number
// scalastyle:off multiple.string.literals

class SqlBuilderSuite extends FunSuite {

  val sqlBuilder = new SqlBuilder
  import sqlBuilder._ // scalastyle:ignore

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
    test(s"logical plan: $result") {
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

  testBuildSelect[Expression, Expression](
    "SELECT SUBSTRING(\"a\", 0, 1) AS \"aa\", \"b\" FROM \"table\" WHERE (\"a\" = 'a')")(
      "table", Seq(Alias(Substring(
        AttributeReference("a", StringType, nullable = true)(),
        Literal(0, IntegerType),
        Literal(1, IntegerType)), "aa")(
          exprId = ExprId(99999L)
        ),
        AttributeReference("b", IntegerType, nullable = true)()
      ),
        Seq(
          expressions.EqualTo(AttributeReference("a", StringType, nullable = true)(),
          Literal("a", StringType)))
      )

  testBuildSelect[Expression, Expression](
    """SELECT SUBSTRING("a", 0, 1) AS "aa", "b"
      |FROM "table"
      |WHERE ("c" = SUBSTRING("a", 0, 2))""".stripMargin
        .replace("\n", " ").replaceAll("\\s+", " "))(
        "table",
        Seq(Alias(Substring(
          AttributeReference("a", StringType)(),
          Literal(0, IntegerType),
          Literal(1, IntegerType)), "aa")(),
          AttributeReference("b", IntegerType)()),
        Seq(expressions.EqualTo(
          AttributeReference("c", StringType)(),
          Substring(
            AttributeReference("a", StringType)(),
            Literal(0, IntegerType),
            Literal(2, IntegerType))))
      )

  testBuildSelect[Expression, Expression]("SELECT COUNT(1) AS \"PartialCount\" FROM \"table\"")(
    "table",
    Seq(Alias(Count(Literal(1, IntegerType)), "PartialCount")(exprId = ExprId(99999L))),
    Nil
  )

  test("expression combinations") {
    /**
     * XXX: EIDXXXXX prefix is added based on ExprId to avoid alias collision
     *
     * Create a semi-automated test for combination of expressions
     */
    val testCases: Map[String, NamedExpression] =
      Map(
        "AVG(1) AS \"PartialAvg\"" ->
            Alias(Average(Literal(1, IntegerType)), "PartialAvg")(exprId = ExprId(99999L)),
        "SUM(1) AS \"PartialSum\"" ->
            Alias(Sum(Literal(1, IntegerType)), "PartialSum")(exprId = ExprId(99999L)),
        "COUNT(1) AS \"PartialCount\"" ->
            Alias(Count(Literal(1, IntegerType)), "PartialCount")(exprId = ExprId(99999L)),
        "MAX(1) AS \"PartialMax\"" ->
            Alias(Max(Literal(1, IntegerType)), "PartialMax")(exprId = ExprId(99999L)),
        "MIN(1) AS \"PartialMin\"" ->
            Alias(Min(Literal(1, IntegerType)), "PartialMin")(exprId = ExprId(99999L))
      )


    testCases.keys.foreach(
      stringRepr =>
        assertResult( s"""SELECT $stringRepr FROM "table"""")(
          sqlBuilder.buildSelect(
            "table", Seq(testCases.getOrElse(stringRepr, null)),
            Nil: Seq[String], Nil: Seq[String]
          )
        )
    )
  }

  testBuildSelect[Expression,Expression,Expression](
    "SELECT \"a\", COUNT(1) FROM \"table\" GROUP BY \"a\"")(
      "table", Seq(AttributeReference("a", StringType)(), Count(Literal(1))),
        Nil, Seq(AttributeReference("a", StringType)())
      )

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

  testLogicalPlan("SELECT * FROM \"t1\"")(Project(Seq(ResolvedStar(Nil)), t1))

  testLogicalPlan(
    s"""SELECT "t1"."c1", "t2"."c2" FROM "t1" INNER JOIN "t2" ON ("t1"."c1" = "t2"."c2")"""
  )(Project(
    AttributeReference("c1", StringType)(qualifiers = "t1" :: Nil) ::
      AttributeReference("c2", StringType)(qualifiers = "t2" :: Nil) :: Nil,
    Join(t1, t2, Inner, Some(expressions.EqualTo(
      AttributeReference("c1", StringType)(qualifiers = "t1" :: Nil),
      AttributeReference("c2", StringType)(qualifiers = "t2" :: Nil)
    )))
  ))

  case object UnsupportedLogicalPlan extends LeafNode {
    override def output: Seq[Attribute] = Seq()
  }
  testUnsupportedLogicalPlan(UnsupportedLogicalPlan)

}
