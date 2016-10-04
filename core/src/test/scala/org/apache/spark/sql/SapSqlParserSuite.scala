package org.apache.spark.sql

import com.sap.spark.PlanTest
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.tablefunctions.UnresolvedTableFunction
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{SimpleCatalystConf, TableIdentifier}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.parser.{SapDQLParser, SapParserDialect, SapParserException}
import org.apache.spark.sql.sources.commands.{Orc, Parquet, UnresolvedInferSchemaCommand}
import org.apache.spark.sql.sources.{CubeViewKind, DimensionViewKind, PlainViewKind}
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
        | JOIN PRIOR u ON v.pred = u.succ
        | ORDER SIBLINGS BY ord
        | START WHERE pred IS NULL
        | SET Node
        |) AS H
      """.stripMargin)

    val expected = Project(AliasUnresolver(Literal(1)), Subquery("H", Hierarchy(
      AdjacencyListHierarchySpec(
        source = UnresolvedRelation(TableIdentifier("T1"), Some("v")),
        parenthoodExp = EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
        childAlias = "u",
        startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
        orderBy = SortOrder(UnresolvedAttribute("ord"), Ascending) :: Nil),
      node = UnresolvedAttribute("Node")
    )))
    comparePlans(expected, result)

    val analyzed = analyzer.execute(result)
    log.info(s"$analyzed")
  }

  // variables for raw select tests
  val rawSqlString = "SELECT something bla FROM A"
  val className = "class.name"

  test("CREATE VIEW with RAW SQL") {
    val parsed = SapDQLParser.parse(
      """CREATE VIEW v AS
        |``SELECT * FROM foo`` USING com.sap.spark.engines
      """.stripMargin)

    assertResult(
      CreateNonPersistentViewCommand(
        PlainViewKind,
        TableIdentifier("v"),
        UnresolvedSelectUsing("SELECT * FROM foo", "com.sap.spark.engines"),
        temporary = false))(parsed)
  }

  test("CREATE VIEW with RAW SQL in Subquery") {
    val parsed = SapDQLParser.parse(
      """CREATE VIEW v AS SELECT * FROM
        |(``SELECT * FROM foo`` USING com.sap.spark.engines) AS t
      """.stripMargin)

    assertResult(
      CreateNonPersistentViewCommand(
        PlainViewKind,
        TableIdentifier("v"),
        Project(
          Seq(UnresolvedAlias(UnresolvedStar(None))),
          Subquery(
            "t",
            UnresolvedSelectUsing("SELECT * FROM foo", "com.sap.spark.engines"))),
        temporary = false))(parsed)
  }

  test ("RAW SQL: ``select ....`` USING class.name") {
    // ('SQL COMMANDO FROM A' USING com.sap.spark.engines) JOIN SELECT * FROM X
    assert(SapDQLParser.parse(s"``$rawSqlString`` USING $className")
      .equals(UnresolvedSelectUsing(rawSqlString, className)))
  }

  test ("RAW SQL: ``select ....`` USING class.name AS (schema)") {
    val schema = "(a integer, b double)"
    val schemaFields = StructType(Seq(StructField("a", IntegerType), StructField("b", DoubleType)))

    // ('SQL COMMANDO FROM A' USING com.sap.spark.engines) JOIN SELECT * FROM X
    assert(SapDQLParser.parse(s"``$rawSqlString`` USING $className AS $schema")
      .equals(UnresolvedSelectUsing(rawSqlString, className, Some(schemaFields))))
  }

  test ("RAW SQL: ``select ....`` USING class.name AS () - empty schema") {
    // ('SQL COMMANDO FROM A' USING com.sap.spark.engines) JOIN SELECT * FROM X
    assert(SapDQLParser.parse(s"``$rawSqlString`` USING $className AS ()")
      .equals(UnresolvedSelectUsing(rawSqlString, className, Some(StructType(Seq.empty)))))
  }

  test ("RAW SQL: ``select ....`` USING class.name OPTIONS ... ()") {
    assertResult(UnresolvedSelectUsing(rawSqlString, className, None, Map("foo" -> "bar")))(
      SapDQLParser.parse(
        s"""``$rawSqlString``
           |USING $className
           |OPTIONS (
           |foo "bar"
           |)
         """.stripMargin)
    )
  }

  test("RawSQL with only one ` should fail!") {
    intercept[SapParserException](SapDQLParser.parse(s"""`select ...` USING $className AS ()"""))
  }

  test("parse system table") {
    val parsed = SapDQLParser.parse("SELECT * FROM SYS.TABLES USING com.sap.spark")

    assertResult(Project(                               // SELECT
      Seq(UnresolvedAlias(UnresolvedStar(None))),       // *
      UnresolvedProviderBoundSystemTable("TABLES",      // FROM SYS.TABLES
        "com.sap.spark", Map.empty)                     // USING com.sap.spark
    ))(parsed)
  }

  test("parse system table with SYS_ prefix (VORASPARK-277)") {
    val statements =
      "SELECT * FROM SYS_TABLES USING com.sap.spark" ::
      "SELECT * FROM sys_TABLES USING com.sap.spark" ::
      "SELECT * FROM SyS_TABLES USING com.sap.spark" :: Nil

    val parsedStatements = statements.map(SapDQLParser.parse)

    parsedStatements.foreach { parsed =>
      assertResult(Project(                               // SELECT
        Seq(UnresolvedAlias(UnresolvedStar(None))),       // *
        UnresolvedProviderBoundSystemTable("TABLES",      // FROM SYS.TABLES
          "com.sap.spark", Map.empty)                     // USING com.sap.spark
      ))(parsed)
    }
  }

  test("parse system table with options") {
    val parsed = SapDQLParser.parse("SELECT * FROM SYS.TABLES " +
      "USING com.sap.spark OPTIONS (foo \"bar\")")

    assertResult(Project(                               // SELECT
      Seq(UnresolvedAlias(UnresolvedStar(None))),       // *
      UnresolvedProviderBoundSystemTable("TABLES",      // FROM SYS.TABLES
        "com.sap.spark", Map("foo" -> "bar"))     // USING com.sap.spark
    ))(parsed)
  }

  test("parse table function") {
    val parsed = SapDQLParser.parse("SELECT * FROM describe_table(SELECT * FROM persons)")

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
      SapDQLParser.parse("SELECT * FROM describe_table(persons)")
    }
  }

  test("'order siblings by' with multiple expressions") {
    val parser = new SapParserDialect
    val result = parser.parse(
      """
        |SELECT 1 FROM HIERARCHY (
        | USING T1 AS v
        | JOIN PRIOR u ON v.pred = u.succ
        | ORDER SIBLINGS BY myAttr ASC, otherAttr DESC, yetAnotherAttr
        | START WHERE pred IS NULL
        | SET Node
        |) AS H
      """.stripMargin)
    val expected = Project(AliasUnresolver(Literal(1)), Subquery("H", Hierarchy(
      AdjacencyListHierarchySpec(
        source = UnresolvedRelation(TableIdentifier("T1"), Some("v")),
        parenthoodExp = EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
        childAlias = "u",
        startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
        orderBy = SortOrder(UnresolvedAttribute("myAttr"), Ascending) ::
          SortOrder(UnresolvedAttribute("otherAttr"), Descending) ::
          SortOrder(UnresolvedAttribute("yetAnotherAttr"), Ascending) :: Nil
      ),
      node = UnresolvedAttribute("Node")
    )))
    comparePlans(expected, result)

    val analyzed = analyzer.execute(result)
    log.info(s"$analyzed")
  }

    test("no 'order siblings by' clause") {
      val parser = new SapParserDialect
      val result = parser.parse(
        """
          |SELECT 1 FROM HIERARCHY (
          | USING T1 AS v
          | JOIN PRIOR u ON v.pred = u.succ
          | START WHERE pred IS NULL
          | SET Node
          |) AS H
        """.stripMargin)
      val expected = Project(AliasUnresolver(Literal(1)), Subquery("H", Hierarchy(
        AdjacencyListHierarchySpec(
          source = UnresolvedRelation(TableIdentifier("T1"), Some("v")),
          parenthoodExp = EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
          childAlias = "u",
          startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
          orderBy = Nil
        ), node = UnresolvedAttribute("Node")
      )))
      comparePlans(expected, result)

      val analyzed = analyzer.execute(result)
      log.info(s"$analyzed")
  }

  test("create temporary view") {
    val parser = new SapParserDialect
    val result = parser.parse("CREATE TEMPORARY VIEW myview AS SELECT 1 FROM mytable")
    val expected = CreateNonPersistentViewCommand(
      PlainViewKind,
      TableIdentifier("myview"),
      Project(AliasUnresolver(Literal(1)), UnresolvedRelation(TableIdentifier("mytable"))),
      temporary = true)
    comparePlans(expected, result)
  }

  test("create view") {
    val parser = new SapParserDialect
    val result = parser.parse("CREATE VIEW myview AS SELECT 1 FROM mytable")
    val expected = CreateNonPersistentViewCommand(
      PlainViewKind,
      TableIdentifier("myview"),
      Project(AliasUnresolver(Literal(1)), UnresolvedRelation(TableIdentifier("mytable"))),
      temporary = false)
    comparePlans(expected, result)
  }

  test("create temporary view of a hierarchy") {
    val parser = new SapParserDialect
    val result = parser.parse("""
                              CREATE TEMPORARY VIEW HV AS SELECT 1 FROM HIERARCHY (
                                 USING T1 AS v
                                 JOIN PRIOR u ON v.pred = u.succ
                                 START WHERE pred IS NULL
                                 SET Node
                                ) AS H
                              """.stripMargin)
    val expected = CreateNonPersistentViewCommand(
      PlainViewKind,
      TableIdentifier("HV"),
      Project(AliasUnresolver(Literal(1)), Subquery("H", Hierarchy(
        AdjacencyListHierarchySpec(source = UnresolvedRelation(TableIdentifier("T1"), Some("v")),
          parenthoodExp = EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
          childAlias = "u",
          startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
          orderBy = Nil),
        node = UnresolvedAttribute("Node")
      ))), temporary = true)
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

    assert(result.isInstanceOf[CreateNonPersistentViewCommand])

    val createView = result.asInstanceOf[CreateNonPersistentViewCommand]

    assertResult(createView.identifier.table)("aview")

    assert(createView.kind == PlainViewKind)

    assert(createView.plan.isInstanceOf[Project])

    val projection = createView.plan.asInstanceOf[Project]

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
      DimensionViewKind, TableIdentifier("myview"),
      Project(AliasUnresolver(Literal(1)),
        UnresolvedRelation(TableIdentifier("mytable"))), temporary = false)
    comparePlans(expected, result)
  }

  test("create incorrect dimension view is handled correctly") {
    assertFailingQuery[SapParserException]("CREATE DIMENSI VIEW myview AS SELECT 1 FROM mytable")
    assertFailingQuery[SapParserException]("CREATE DIMENSION myview AS SELECT 1 FROM mytable")
  }

  test("Parse correct CREATE VIEW USING") {
    val parser = new SapParserDialect
    val statement = "CREATE VIEW v AS SELECT * FROM t USING com.sap.spark.vora"

    val parsed = parser.parse(statement)
    assert(parsed.isInstanceOf[CreatePersistentViewCommand])

    val actual = parsed.asInstanceOf[CreatePersistentViewCommand]
    assert(actual.kind == PlainViewKind)
    assertResult(statement)(actual.viewSql)
    assertResult(Project(UnresolvedAlias(UnresolvedStar(None)) :: Nil,
      UnresolvedRelation(TableIdentifier("t"))))(actual.plan)
    assertResult(false)(actual.allowExisting)
    assertResult(TableIdentifier("v"))(actual.identifier)
    assertResult("com.sap.spark.vora")(actual.provider)
    assertResult(Map.empty)(actual.options)
  }

  test("Parse correct CREATE VIEW USING with sub-select (bug 105558") {
    val parser = new SapParserDialect
    val statement = "CREATE VIEW v AS SELECT sq.a FROM " +
      "(SELECT * FROM t) sq USING com.sap.spark.vora"

    val parsed = parser.parse(statement)
    assert(parsed.isInstanceOf[CreatePersistentViewCommand])

    val actual = parsed.asInstanceOf[CreatePersistentViewCommand]
    assert(actual.kind == PlainViewKind)

    assertResult(
      Project(
        UnresolvedAlias(UnresolvedAttribute(Seq("sq", "a"))) :: Nil,
        Subquery(
          "sq",
          Project(
            UnresolvedAlias(UnresolvedStar(None)) :: Nil,
            UnresolvedRelation(TableIdentifier("t"), None)
          )
        )
      ))(actual.plan)
    assertResult(statement)(actual.viewSql)
    assertResult(false)(actual.allowExisting)
    assertResult(TableIdentifier("v"))(actual.identifier)
    assertResult("com.sap.spark.vora")(actual.provider)
    assertResult(Map.empty)(actual.options)
  }

  test("Parse correct CREATE VIEW USING OPTIONS") {
    val parser = new SapParserDialect
    val statement = """CREATE VIEW IF NOT EXISTS v
                      |AS SELECT * FROM t
                      |USING com.sap.spark.vora
                      |OPTIONS(discovery "1.1.1.1")""".stripMargin

    val parsed = parser.parse(statement)
    assert(parsed.isInstanceOf[CreatePersistentViewCommand])

    val actual = parsed.asInstanceOf[CreatePersistentViewCommand]
    assert(actual.kind == PlainViewKind)
    assertResult(Project(UnresolvedAlias(UnresolvedStar(None)) :: Nil,
      UnresolvedRelation(TableIdentifier("t"))))(actual.plan)
    assertResult(true)(actual.allowExisting)
    assertResult(statement)(actual.viewSql)
    assertResult(TableIdentifier("v"))(actual.identifier)
    assertResult("com.sap.spark.vora")(actual.provider)
    assertResult(Map[String, String]("discovery" -> "1.1.1.1"))(actual.options)
  }

  test("Parse correct CREATE VIEW USING with annotations") {
    val parser = new SapParserDialect
    val statement = """CREATE VIEW IF NOT EXISTS v
                      |AS SELECT a as al @ ( b = 'c' ) FROM t
                      |USING com.sap.spark.vora
                      |OPTIONS(discovery "1.1.1.1")""".stripMargin

    val parsed = parser.parse(statement)
    assert(parsed.isInstanceOf[CreatePersistentViewCommand])
    val persistedViewCommand = parsed.asInstanceOf[CreatePersistentViewCommand]
    assertResult(persistedViewCommand.identifier.table)("v")
    assertResult(statement)(persistedViewCommand.viewSql)
    assert(persistedViewCommand.kind == PlainViewKind)
    assert(persistedViewCommand.plan.isInstanceOf[Project])
    val projection = persistedViewCommand.plan.asInstanceOf[Project]

    assertResult(UnresolvedRelation(TableIdentifier("t")))(projection.child)

    val expected = Seq(
      ("al", UnresolvedAttribute("a"), Map("b" -> Literal.create("c", StringType))))

    assertAnnotatedProjection(expected)(projection.projectList)

    assertResult("com.sap.spark.vora")(persistedViewCommand.provider)
    assertResult(Map[String, String]("discovery" -> "1.1.1.1"))(persistedViewCommand.options)
  }

  test("Handle incorrect CREATE VIEW statements") {
    val parser = new SapParserDialect
    val invStatement1 =
      """CREATE VIE v AS SELECT * FROM t USING com.sap.spark.vora
      """.stripMargin
    intercept[SapParserException](parser.parse(invStatement1))

    val invStatement2 =
      """CREATE VIEW v AS SELEC * FROM t USING com.sap.spark.vora
      """.stripMargin
    intercept[SapParserException](parser.parse(invStatement2))

    val invStatement3 =
      """CREATE VIEW v AS SELECT * FROM t USIN com.sap.spark.vora
      """.stripMargin
    intercept[SapParserException](parser.parse(invStatement3))

    val invStatement5 =
      """CREATE VIEW v AS SELECT USING com.sap.spark.vora
      """.stripMargin
    intercept[SapParserException](parser.parse(invStatement5))
  }

  test("Parse correct CREATE DIMENSION VIEW USING OPTIONS") {
    val parser = new SapParserDialect
    val statement = """CREATE DIMENSION VIEW IF NOT EXISTS v
                      |AS SELECT * FROM t
                      |USING com.sap.spark.vora
                      |OPTIONS(discovery "1.1.1.1")""".stripMargin

    val parsed = parser.parse(statement)
    assert(parsed.isInstanceOf[CreatePersistentViewCommand])

    val actual = parsed.asInstanceOf[CreatePersistentViewCommand]
    assert(actual.kind == DimensionViewKind)
    assertResult(Project(UnresolvedAlias(UnresolvedStar(None)) :: Nil,
      UnresolvedRelation(TableIdentifier("t"))))(actual.plan)
    assertResult(true)(actual.allowExisting)
    assertResult(TableIdentifier("v"))(actual.identifier)
    assertResult("com.sap.spark.vora")(actual.provider)
    assertResult(Map[String, String]("discovery" -> "1.1.1.1"))(actual.options)
  }

  test("Handle incorrect CREATE DIMENSION VIEW statements") {
    val parser = new SapParserDialect
    val invStatement1 =
      """CREATE DIMENSI VIEW v AS SELECT * FROM t USING com.sap.spark.vora
      """.stripMargin
    intercept[SapParserException](parser.parse(invStatement1))

    val invStatement2 =
      """CREATE DIMNESION v AS SELEC * FROM t USING com.sap.spark.vora
      """.stripMargin
    intercept[SapParserException](parser.parse(invStatement2))

    val invStatement3 =
      """CREATE DIMNESION VIEW v AS SELECT * FROM t USIN com.sap.spark.vora
      """.stripMargin
    intercept[SapParserException](parser.parse(invStatement3))

    val invStatement5 =
      """CREATE DIMNESION VIEW v AS SELECT USING com.sap.spark.vora
      """.stripMargin
    intercept[SapParserException](parser.parse(invStatement5))
  }

  test("Parse correct CREATE CUBE VIEW USING OPTIONS") {
    val parser = new SapParserDialect
    val statement = """CREATE CUBE VIEW IF NOT EXISTS v
                      |AS SELECT * FROM t
                      |USING com.sap.spark.vora
                      |OPTIONS(discovery "1.1.1.1")""".stripMargin

    val parsed = parser.parse(statement)
    assert(parsed.isInstanceOf[CreatePersistentViewCommand])

    val actual = parsed.asInstanceOf[CreatePersistentViewCommand]
    assertResult(statement)(actual.viewSql)
    assert(actual.kind == CubeViewKind)
    assertResult(Project(UnresolvedAlias(UnresolvedStar(None)) :: Nil,
      UnresolvedRelation(TableIdentifier("t"))))(actual.plan)
    assertResult(true)(actual.allowExisting)
    assertResult(TableIdentifier("v"))(actual.identifier)
    assertResult("com.sap.spark.vora")(actual.provider)
    assertResult(Map[String, String]("discovery" -> "1.1.1.1"))(actual.options)
  }

  test("Handle incorrect CREATE CUBE VIEW statements") {
    val parser = new SapParserDialect
    val invStatement1 =
      """CREATE CUBEI VIEW v AS SELECT * FROM t USING com.sap.spark.vora
      """.stripMargin
    intercept[SapParserException](parser.parse(invStatement1))

    val invStatement2 =
      """CREATE CIUBE v AS SELEC * FROM t USING com.sap.spark.vora
      """.stripMargin
    intercept[SapParserException](parser.parse(invStatement2))

    val invStatement3 =
      """CREATE CBE VIEW v AS SELECT * FROM t USIN com.sap.spark.vora
      """.stripMargin
    intercept[SapParserException](parser.parse(invStatement3))

    val invStatement5 =
      """CREATE CUBEN VIEW v AS SELECT USING com.sap.spark.vora
      """.stripMargin
    intercept[SapParserException](parser.parse(invStatement5))
  }

  test("IF function can be used") {
    val parsed = SapDQLParser.parse("SELECT IF(1 = 1) FROM baz")
    assertResult(
      Project(
        Seq(
          UnresolvedAlias(
            UnresolvedFunction("IF", Seq(EqualTo(Literal(1), Literal(1))), isDistinct = false))),
        UnresolvedRelation(TableIdentifier("baz"))))(parsed)
  }

  test("Infer schema command with explicit types") {
    val parsed1 = SapDQLParser.parse("""INFER SCHEMA OF "foo" AS orc""")
    val parsed2 = SapDQLParser.parse("""INFER SCHEMA OF "foo" AS parquet""")

    assertResult(UnresolvedInferSchemaCommand("foo", Some(Orc)))(parsed1)
    assertResult(UnresolvedInferSchemaCommand("foo", Some(Parquet)))(parsed2)
  }

  test("Infer schema command without explicit type") {
    val parsed = SapDQLParser.parse("""INFER SCHEMA OF "foo"""")

    assertResult(UnresolvedInferSchemaCommand("foo", None))(parsed)
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
  def assertFailingQuery[T <: Exception: ClassTag: TypeTag]
        (query: String,
         message: String = "Syntax error"): Unit = {
    val parser = new SapParserDialect
    val ex = intercept[T] {
      parser.parse(query)
    }
    assert(ex.getMessage.contains(message))
  }
}
