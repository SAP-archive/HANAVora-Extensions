package org.apache.spark.sql.views

import com.sap.spark.dstest.DefaultSource
import com.sap.spark.dsmock.DefaultSource.withMock
import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{CreatePersistentViewCommand, ProviderException}
import org.apache.spark.sql.hierarchy.HierarchyTestUtils
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.sql.{Dimension, Plain}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.mock.MockitoSugar
import DatasourceResolver._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.tablefunctions.TPCHTables
import org.apache.spark.sql.types._
import org.mockito.internal.stubbing.answers.Returns
import org.apache.spark.util.DummyRelationUtils._

import scala.util.Random

class ViewsSuite extends FunSuite
  with HierarchyTestUtils
  with GlobalSapSQLContext
  with MockitoSugar
  with Logging
  with Matchers {

  case object Dummy extends LeafNode with NoOutput

  class DummyViewProvider extends ViewProvider {
    override def createView(createViewInput: CreateViewInput): ViewHandle =
      new ViewHandle {
        override def drop(): Unit = ()
      }

    override def dropView(dropViewInput: DropViewInput): Unit = ()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    DefaultSource.reset()
  }

  test("View with raw sql works") {
    withMock { defaultSource =>
      val schema: StructType = 'a.string
      val output = schema.toAttributes
      val execution = mock[RawSqlExecution]
      when(execution.output).thenReturn(output)
      when(defaultSource.executionOf(
        any[SQLContext],
        any[Map[String, String]],
        any[String],
        any[Option[StructType]]))
        .thenReturn(execution)

      sqlc.sql("CREATE VIEW v AS ``SELECT * FROM FOO`` USING com.sap.spark.dsmock")
      val resultSchema = sqlc.sql("SELECT * FROM v").schema

      assertResult(schema)(resultSchema)
    }
  }

  test("View with raw sql in subquery works") {
    withMock { defaultSource =>
      val schema: StructType = 'a.string
      val execution = mock[RawSqlExecution]
      when(execution.output).thenReturn(schema.toAttributes)
      when(defaultSource
        .executionOf(
          any[SQLContext],
          any[Map[String, String]],
          any[String],
          any[Option[StructType]]))
        .thenReturn(execution)

      sqlc.sql(
        """CREATE VIEW v AS
          |SELECT * FROM
          |(``SELECT * FROM FOO``
          |USING com.sap.spark.dsmock) t""".stripMargin)
      val resultSchema = sqlc.sql("SELECT * FROM v").schema

      assertResult(schema)(resultSchema)
    }
  }

  test("Create view with min/max works (bug 116822)") {
    val customerTable = new TPCHTables(sqlc).customerTable
    sqlc.baseRelationToDataFrame(customerTable).registerTempTable(customerTable.tableName)

    sqlc.sql(
      """CREATE VIEW RASH_AGGR_1 AS
        |SELECT MIN(C_CUSTKEY) AS KEY, C_MKTSEGMENT
        |FROM CUSTOMER
        |GROUP BY C_MKTSEGMENT
        |USING com.sap.spark.dstest
      """.stripMargin)

    assertResult(
      StructType(
        StructField("KEY", IntegerType) ::
        StructField("C_MKTSEGMENT", StringType) :: Nil))(
      sqlc.sql("SELECT * FROM RASH_AGGR_1").schema)
  }

  test("Create view with year function works (bug 116821)") {
    sqlc.sql(
      """CREATE VIEW v AS
        |SELECT YEAR('2015-04-01') AS col
        |USING com.sap.spark.dstest
      """.stripMargin)

    assertResult(
      StructType(StructField("col", IntegerType) :: Nil))(sqlc.sql("SELECT * FROM v").schema)
  }

  test("Rewire nested view after dropping and recreating nested view (bug 104634)") {
    createOrgTable(sqlContext)
    createAnimalsTable(sqlContext)

    sqlContext.sql(s"CREATE VIEW v1 AS SELECT * FROM $orgTbl")
    sqlContext.sql("CREATE VIEW v2 AS SELECT * FROM v1 WHERE ord = 1")

    // drop the view v1.
    sqlContext.catalog.unregisterTable(TableIdentifier("v1"))

    // create v1 again using different schema.
    sqlContext.sql(s"CREATE VIEW v1 AS SELECT * FROM $animalsTable")

    // now v2 should work using the updated v1.
    val result = sqlContext.sql("SELECT * FROM v2").collect()
    assertResult(Seq(
      Row("Animal", null, 1, 1),
      Row("Mammal", 1, 2, 1)).toSet)(result.toSet)
  }

  test("Rewire nested view after dropping and recreating nested table (bug 104634)") {
    createOrgTable(sqlContext)
    sqlContext.sql(s"CREATE VIEW v1 AS SELECT * FROM $orgTbl WHERE ord = 1")

    // drop the table.
    sqlContext.catalog.unregisterTable(TableIdentifier(orgTbl))

    // create the table again using different schema.
    val rdd2 = sc.parallelize(animalsHierarchy.sortBy(x => Random.nextDouble()))
    val animals = sqlContext.createDataFrame(rdd2).cache()
    animals.registerTempTable(orgTbl)

    // now v2 should work using the updated v1.
    val result = sqlContext.sql("SELECT * FROM v1").collect()
    assertResult(Seq(
      Row("Animal", null, 1, 1),
      Row("Mammal", 1, 2, 1)).toSet)(result.toSet)
  }

  test("Rewire nested dimension view after dropping and recreating " +
    "nested dimension view (bug 104634)") {
    createOrgTable(sqlContext)
    createAnimalsTable(sqlContext)

    sqlContext.sql(s"CREATE DIMENSION VIEW v1 AS SELECT * FROM $orgTbl")
    sqlContext.sql("CREATE DIMENSION VIEW v2 AS SELECT * FROM v1 WHERE ord = 1")

    // drop the dimension view v1.
    sqlContext.catalog.unregisterTable(TableIdentifier("v1"))

    // create dimension v1 again using different schema.
    sqlContext.sql(s"CREATE DIMENSION VIEW v1 AS SELECT * FROM $animalsTable")

    // now v2 should work using the updated v1.
    val result = sqlContext.sql("SELECT * FROM v2").collect()
    assertResult(Seq(
      Row("Animal", null, 1, 1),
      Row("Mammal", 1, 2, 1)).toSet)(result.toSet)
  }

  test("Views names case-sensitivity is handled correctly") {
    createOrgTable(sqlContext)

    val originalConf = sqlContext.conf.caseSensitiveAnalysis

    try {
      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, "false")
      sqlContext.sql(s"CREATE VIEW VIEW1 AS SELECT * FROM $orgTbl")
      intercept[RuntimeException] {
        sqlContext.sql(s"CREATE VIEW view1 AS SELECT * FROM $orgTbl")
      }

      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, "true")
      sqlContext.sql(s"CREATE VIEW VIEW2 AS SELECT * FROM $orgTbl")
      // this will not throw (although we use the same identifier name).
      sqlContext.sql(s"CREATE VIEW view2 AS SELECT * FROM $orgTbl")
    } finally {
      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, originalConf.toString)
    }
  }

  test("CREATE VIEW IF NOT EXISTS handles case-sensitivity correctly") {
    createOrgTable(sqlContext)
    val originalConf = sqlContext.conf.caseSensitiveAnalysis
    try {
      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, "false")
      sqlContext.sql(s"CREATE VIEW view1 AS SELECT * FROM $orgTbl USING com.sap.spark.dstest")
      // should not throw.
      sqlContext.sql("CREATE VIEW IF NOT EXISTS VIEW1 AS SELECT * " +
        s"FROM $orgTbl USING com.sap.spark.dstest")

      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, "true")

      sqlContext.sql(s"CREATE VIEW view2 AS SELECT * FROM $orgTbl USING com.sap.spark.dstest")
      // should not throw.
      sqlContext.sql("CREATE VIEW IF NOT EXISTS VIEW2 AS SELECT * " +
        s"FROM $orgTbl USING com.sap.spark.dstest")
    } finally {
      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, originalConf.toString)
    }
  }

  test("CREATE DIMENSION VIEW IF NOT EXISTS handles case-sensitivity correctly") {
    createOrgTable(sqlContext)

    val originalConf = sqlContext.conf.caseSensitiveAnalysis

    try {
      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, "false")
      sqlContext.sql("CREATE DIMENSION VIEW view1 AS SELECT * " +
        s"FROM $orgTbl USING com.sap.spark.dstest")
      // should not throw.
      sqlContext.sql("CREATE DIMENSION VIEW IF NOT EXISTS VIEW1 AS SELECT * " +
        s"FROM $orgTbl USING com.sap.spark.dstest")

      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, "true")

      sqlContext.sql("CREATE DIMENSION VIEW view2 AS SELECT * " +
        s"FROM $orgTbl USING com.sap.spark.dstest")
      // should not throw.
      sqlContext.sql("CREATE DIMENSION VIEW IF NOT EXISTS VIEW2 AS SELECT * " +
        s"FROM $orgTbl USING com.sap.spark.dstest")

    } finally {
      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, originalConf.toString)
    }
  }

  test("Prohibit definition of recursive views (Bug 106028)") {
    intercept[AnalysisException] {
      sqlContext.sql("CREATE VIEW v AS SELECT * FROM v")
    }
  }

  test("Put correct view logical plan in catalog") {
    sqlContext.sql("CREATE TABLE t USING com.sap.spark.dstest")
    // add the table name manually to dstest's 'catalog'.
    com.sap.spark.dstest.DefaultSource.addRelation("t")
    sqlContext.sql("CREATE VIEW v AS SELECT * FROM t USING com.sap.spark.dstest")

    sqlContext.catalog.unregisterAllTables()
    sqlContext.sql("REGISTER ALL TABLES USING com.sap.spark.dstest")

    val actual = sqlContext.catalog.lookupRelation(TableIdentifier("v"))

    assertResult(Subquery(
      "v",
      PersistedView(
        Project(
          UnresolvedAlias(UnresolvedStar(None)) :: Nil,
          UnresolvedRelation(TableIdentifier("t"))),
        DefaultSource.DropViewHandle("v", "view"),
        "com.sap.spark.dstest"
      )))(actual)
  }

  test("Valid view provider is issued to create view") {
    val provider = spy(new DummyViewProvider)

    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOf("qux")).thenAnswer(new Returns(provider))

    withResolver(sqlContext, resolver) {
      val viewCommand =
        CreatePersistentViewCommand(Plain, TableIdentifier("foo"), Dummy,
          "view_sql", "qux", Map.empty, allowExisting = true)

      viewCommand.run(sqlContext)
      verify(provider, times(1)).toSingleViewProvider
      verify(provider, times(1))
        .createView(CreateViewInput(sqlContext, Dummy, "view_sql", Map.empty,
          TableIdentifier("foo"), allowExisting = true))
    }
  }

  test("Upon invalid providers, an exception is thrown") {
    val provider = spy(new DummyViewProvider)
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOf("qux")).thenAnswer(new Returns(provider))

    withResolver(sqlContext, resolver) {
      val viewCommand =
        CreatePersistentViewCommand(Dimension, TableIdentifier("foo"), Dummy,
          "view_sql", "qux", Map.empty, allowExisting = true)

      intercept[ProviderException] {
        viewCommand.run(sqlContext)
      }
    }
  }

  test("Create view stores a view in the provider") {
    createAnimalsTable(sqlc)

    sqlc.sql(s"CREATE VIEW v As SELECT * FROM $animalsTable USING com.sap.spark.dstest")

    val tables = sqlc.sql("SHOW TABLES USING com.sap.spark.dstest").collect()
    tables should contain(Row("v", "FALSE", "VIEW"))
  }

  test("Create dimension view stores a dimension view in the provider") {
    createAnimalsTable(sqlc)

    sqlc.sql(s"CREATE DIMENSION VIEW v As SELECT * FROM $animalsTable USING com.sap.spark.dstest")

    val tables = sqlc.sql("SHOW TABLES USING com.sap.spark.dstest").collect()
    tables should contain(Row("v", "FALSE", "DIMENSION"))
  }

  test("Create view with invalid provider throws") {
    createAnimalsTable(sqlc)

    intercept[ProviderException] {
      sqlc.sql(s"CREATE CUBE VIEW v AS SELECT * FROM $animalsTable USING com.sap.spark.dstest")
    }
  }

  test("Drop view drops the view from the provider") {
    createAnimalsTable(sqlc)
    sqlc.sql(s"CREATE DIMENSION VIEW v AS SELECT * FROM $animalsTable USING com.sap.spark.dstest")
    val beforeDrop = sqlc.sql("SHOW TABLES USING com.sap.spark.dstest").collect()
    beforeDrop should contain(Row("v", "FALSE", "DIMENSION"))

    sqlc.sql(s"DROP DIMENSION VIEW v USING com.sap.spark.dstest")

    val afterDrop = sqlc.sql("SHOW TABLES USING com.sap.spark.dstest").collect()
    afterDrop should not contain Row("v", "FALSE", "DIMENSION")
  }

  test("Create a persistent hierarchy view (bug 108710)") {
    createOrgTable(sqlContext)
    sqlContext.sql(s"""CREATE VIEW v1 AS SELECT *
                       FROM HIERARCHY
                       (USING $orgTbl AS v JOIN PRIOR u ON v.pred = u.succ
                       ORDER SIBLINGS BY ord ASC
                       START WHERE pred IS NULL
                       SET node) AS H
                       USING com.sap.spark.dstest""")
    val actual = sqlContext.catalog.lookupRelation(TableIdentifier("v1"))
    assertResult(Subquery("v1",
      PersistedView(
        Project(UnresolvedAlias(UnresolvedStar(None)) :: Nil,
          Subquery("H",
            Hierarchy(
              AdjacencyListHierarchySpec(UnresolvedRelation(TableIdentifier("organizationTbl"),
                Some("v")),"u",
                EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
                Some(IsNull(UnresolvedAttribute("pred"))),
                SortOrder(UnresolvedAttribute("ord"), Ascending) :: Nil),
              UnresolvedAttribute("node")
            ))),
        DefaultSource.DropViewHandle("v1", "view"),
        "com.sap.spark.dstest"
    )))(actual)
  }
}
