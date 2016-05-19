package org.apache.spark.sql

import com.sap.spark.PlanTest
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.{SimpleCatalystConf, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.tablefunctions.UnresolvedTableFunction
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.CreateNonPersistentViewCommand
import org.apache.spark.sql.types._
import org.apache.spark.util.AnnotationParsingUtils
import org.scalatest.FunSuite

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class SapSqlParserSuite
  extends FunSuite
  with PlanTest
  with AnnotationParsingUtils
  with Logging {

  def t1: LogicalPlan = new LocalRelation(output = Seq(
    new AttributeReference("pred", StringType, nullable = true, metadata = Metadata.empty)(),
    new AttributeReference("succ", StringType, nullable = false, metadata = Metadata.empty)(),
    new AttributeReference("ord", StringType, nullable = false, metadata = Metadata.empty)()
  ).map(_.toAttribute)
  )

  def catalog: Catalog = {
    val catalog = new SimpleCatalog(SimpleCatalystConf(true))
    catalog.registerTable(TableIdentifier("T1"), t1)
    catalog
  }

  def analyzer: Analyzer = new Analyzer(catalog, EmptyFunctionRegistry, SimpleCatalystConf(true))
  test("basic case") {
    val parser = new SapParserDialect
    val result = parser.parse(
      """
        |SELECT 1 FROM HIERARCHY (
        | USING T1 AS v
        | JOIN PARENT u ON v.pred = u.succ
        | SEARCH BY ord
        | START WHERE pred IS NULL
        | SET Node
        |) AS H
      """.stripMargin)

    val expected = Project(AliasUnresolver(Literal(1)), Subquery("H", Hierarchy(
      relation = UnresolvedRelation(TableIdentifier("T1"), Some("v")),
      parenthoodExpression = EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
      childAlias = "u",
      startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
      searchBy = SortOrder(UnresolvedAttribute("ord"), Ascending) :: Nil,
      nodeAttribute = UnresolvedAttribute("Node")
    )))
    comparePlans(expected, result)

    val analyzed = analyzer.execute(result)
    log.info(s"$analyzed")
  }

  test ("RAW SQL: 'select ....' WITH class.name") {
    val rawSqlString = "SELECT something bla FROM A"
    val className = "class.name"

    assert(SapSqlParser.parse(s"'$rawSqlString' WITH $className")
      .equals(UnresolvedSelectWith(rawSqlString, className)))
  }

  test("parse system table") {
    val parsed = SapSqlParser.parse("SELECT * FROM SYS.TABLES USING com.sap.spark")

    assertResult(Project(                               // SELECT
      Seq(UnresolvedAlias(UnresolvedStar(None))),       // *
      UnresolvedSystemTable("TABLES",                   // FROM SYS.TABLES
        "com.sap.spark", Map.empty)               // USING com.sap.spark
    ))(parsed)
  }

  test("parse system table with options") {
    val parsed = SapSqlParser.parse("SELECT * FROM SYS.TABLES " +
      "USING com.sap.spark OPTIONS (foo \"bar\")")

    assertResult(Project(                               // SELECT
      Seq(UnresolvedAlias(UnresolvedStar(None))),       // *
      UnresolvedSystemTable("TABLES",                   // FROM SYS.TABLES
        "com.sap.spark", Map("foo" -> "bar"))     // USING com.sap.spark
    ))(parsed)
  }

  test("parse table function") {
    val parsed = SapSqlParser.parse("SELECT * FROM describe_table(SELECT * FROM persons)")

    assert(parsed == Project(                           // SELECT
      Seq(UnresolvedAlias(UnresolvedStar(None))),       // *
      UnresolvedTableFunction("describe_table",         // FROM describe_table(
        Seq(Project(                                    // SELECT
          Seq(UnresolvedAlias(UnresolvedStar(None))),   // *
          UnresolvedRelation(TableIdentifier("persons"))            // FROM persons
        )))))                                           // )
  }

  test("fail on incorrect table function") {
    // This should fail since a projection statement is required
    intercept[SapParserException] {
      SapSqlParser.parse("SELECT * FROM describe_table(persons)")
    }
  }

  test("search by with multiple search by expressions") {
    val parser = new SapParserDialect
    val result = parser.parse(
      """
        |SELECT 1 FROM HIERARCHY (
        | USING T1 AS v
        | JOIN PARENT u ON v.pred = u.succ
        | SEARCH BY myAttr ASC, otherAttr DESC, yetAnotherAttr
        | START WHERE pred IS NULL
        | SET Node
        |) AS H
      """.stripMargin)
    val expected = Project(AliasUnresolver(Literal(1)), Subquery("H", Hierarchy(
      relation = UnresolvedRelation(TableIdentifier("T1"), Some("v")),
      parenthoodExpression = EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
      childAlias = "u",
      startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
      searchBy = SortOrder(UnresolvedAttribute("myAttr"), Ascending) ::
        SortOrder(UnresolvedAttribute("otherAttr"), Descending) ::
        SortOrder(UnresolvedAttribute("yetAnotherAttr"), Ascending) :: Nil,
      nodeAttribute = UnresolvedAttribute("Node")
    )))
    comparePlans(expected, result)

    val analyzed = analyzer.execute(result)
    log.info(s"$analyzed")
  }

    test("search by with no search by") {
      val parser = new SapParserDialect
      val result = parser.parse(
        """
          |SELECT 1 FROM HIERARCHY (
          | USING T1 AS v
          | JOIN PARENT u ON v.pred = u.succ
          | START WHERE pred IS NULL
          | SET Node
          |) AS H
        """.stripMargin)
      val expected = Project(AliasUnresolver(Literal(1)), Subquery("H", Hierarchy(
        relation = UnresolvedRelation(TableIdentifier("T1"), Some("v")),
        parenthoodExpression =
          EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
        childAlias = "u",
        startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
        searchBy = Nil,
        nodeAttribute = UnresolvedAttribute("Node")
      )))
      comparePlans(expected, result)

      val analyzed = analyzer.execute(result)
      log.info(s"$analyzed")
  }

  test("create temporary view") {
    val parser = new SapParserDialect
    val result = parser.parse("CREATE TEMPORARY VIEW myview AS SELECT 1 FROM mytable")
    val expected = CreateNonPersistentViewCommand(
      NonPersistedView(Project(AliasUnresolver(Literal(1)),
        UnresolvedRelation(TableIdentifier("mytable")))),
      TableIdentifier("myview"), temporary = true)
    comparePlans(expected, result)
  }

  test("create view") {
    val parser = new SapParserDialect
    val result = parser.parse("CREATE VIEW myview AS SELECT 1 FROM mytable")
    val expected = CreateNonPersistentViewCommand(
      NonPersistedView(Project(AliasUnresolver(Literal(1)),
        UnresolvedRelation(TableIdentifier("mytable")))),
      TableIdentifier("myview"), temporary = false)
    comparePlans(expected, result)
  }

  test("create temporary view of a hierarchy") {
    val parser = new SapParserDialect
    val result = parser.parse("""
                              CREATE TEMPORARY VIEW HV AS SELECT 1 FROM HIERARCHY (
                                 USING T1 AS v
                                 JOIN PARENT u ON v.pred = u.succ
                                 START WHERE pred IS NULL
                                 SET Node
                                ) AS H
                              """.stripMargin)
    val expected = CreateNonPersistentViewCommand(
      NonPersistedView(Project(AliasUnresolver(Literal(1)), Subquery("H", Hierarchy(
        relation = UnresolvedRelation(TableIdentifier("T1"), Some("v")),
        parenthoodExpression =
          EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
        childAlias = "u",
        startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
        searchBy = Nil,
        nodeAttribute = UnresolvedAttribute("Node")
      )))), TableIdentifier("HV"), temporary = true)
    comparePlans(expected, result)
  }


  // scalastyle:off magic.number
  test("create view with annotations") {
    val parser = new SapParserDialect

    val result = parser.parse("CREATE VIEW aview AS SELECT " +
      "A AS AL @ (foo = 'bar')," +
      "B AS BL @ (foo = 'bar', baz = 'bla')," +
      "C AS CL @ (foo = ('bar', 'baz'))," +
      "D AS DL @ (foo = '?')," +
      "E AS EL @ (* = 'something')," +
      "F AS FL @ (foo = null)," +
      "G AS GL @ (foo = 123)," +
      "H AS HL @ (foo = 1.23) " +
      "FROM atable")

    assert(result.isInstanceOf[CreateNonPersistentViewCommand[_]])

    val createView = result.asInstanceOf[CreateNonPersistentViewCommand[_]]

    assertResult(createView.identifier.table)("aview")

    assert(createView.view.isInstanceOf[NonPersistedView])

    val nonPersistedView = createView.view.asInstanceOf[NonPersistedView]

    assert(nonPersistedView.plan.isInstanceOf[Project])

    val projection = nonPersistedView.plan.asInstanceOf[Project]

    assertResult(UnresolvedRelation(TableIdentifier("atable")))(projection.child)

    val actual = projection.projectList

    val expected = Seq(
      ("AL", UnresolvedAttribute("A"), Map("foo" -> Literal.create("bar", StringType))),
      ("BL", UnresolvedAttribute("B"), Map("foo" ->
        Literal.create("bar", StringType), "baz" -> Literal.create("bla", StringType))),
      ("CL", UnresolvedAttribute("C"),
        Map("foo" -> Literal.create("""[bar,baz]""", StringType))),
      ("DL", UnresolvedAttribute("D"), Map("foo" -> Literal.create("?", StringType))),
      ("EL", UnresolvedAttribute("E"), Map("*" -> Literal.create("something", StringType))),
      ("FL", UnresolvedAttribute("F"),
        Map("foo" -> Literal.create(null, NullType))),
      ("GL", UnresolvedAttribute("G"),
        Map("foo" -> Literal.create(123, LongType))),
      ("HL", UnresolvedAttribute("H"),
        Map("foo" -> Literal.create(1.23, DoubleType)))
      )

    assertAnnotatedProjection(expected)(actual)
  }

  test("create table with invalid annotation position") {
    assertFailingQuery[SapParserException]("CREATE VIEW aview AS SELECT A @ (foo = 'bar') " +
      "AS AL FROM atable")
  }

  test("create table with invalid key type") {
    assertFailingQuery[SapParserException]("CREATE VIEW aview AS " +
      "SELECT A AS AL @ (111 = 'bar') FROM atable")
  }

  test("create table with invalid value") {
    assertFailingQuery[SapParserException]("CREATE VIEW aview AS " +
      "SELECT A AS AL @ (key = !@!) FROM atable")
  }

  test("create table with two identical keys") {
    assertFailingQuery[AnalysisException]("CREATE VIEW aview AS " +
      "SELECT A AS AL @ (key1 = 'val1', key1 = 'val2', key2 = 'val1') FROM atable",
      "duplicate keys found: key1")
  }

  test("create table with malformed annotation") {
    assertFailingQuery[SapParserException]("CREATE VIEW aview " +
      "AS SELECT A AS AL @ (key2 ^ 'val1') FROM atable")
  }

  test("create table with an annotation without value") {
    assertFailingQuery[SapParserException]("CREATE VIEW aview " +
      "AS SELECT A AS AL @ (key = ) FROM atable")
  }

  test("create dimension view") {
    val parser = new SapParserDialect
    val result = parser.parse("CREATE DIMENSION VIEW myview AS SELECT 1 FROM mytable")
    val expected = CreateNonPersistentViewCommand(
      NonPersistedDimensionView(Project(AliasUnresolver(Literal(1)),
        UnresolvedRelation(TableIdentifier("mytable")))),
      TableIdentifier("myview"), temporary = false)
    comparePlans(expected, result)
  }

  test("create incorrect dimension view is handled correctly") {
    assertFailingQuery[SapParserException]("CREATE DIMENSI VIEW myview AS SELECT 1 FROM mytable")
    assertFailingQuery[SapParserException]("CREATE DIMENSION myview AS SELECT 1 FROM mytable")
  }

  /**
    * Utility method that creates a [[SapParserDialect]] parser and tries to parse the given
    * query, it expects the parser to fail with the exception type parameter and the exception
    * message should contain the text defined in the message parameter.
    *
    * @param query The query.
    * @param message Part of the expected error message.
    * @tparam T The exception type.
    */
  def assertFailingQuery[T <: Exception: ClassTag: TypeTag](query: String,
                                                   message: String = "Syntax error"): Unit = {
    val parser = new SapParserDialect
    val ex = intercept[T] {
      parser.parse(query)
    }
    assert(ex.getMessage.contains(message))
  }

}
