package org.apache.spark.sql.views

import com.sap.spark.dstest.DefaultSource
import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{CreatePersistentViewCommand, ProviderException}
import org.apache.spark.sql.hierarchy.HierarchyTestUtils
import org.apache.spark.sql.sources._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

import scala.util.Random

class ViewsSuite extends FunSuite
  with HierarchyTestUtils
  with GlobalSapSQLContext
  with MockitoSugar
  with Logging {

  case object Dummy extends LeafNode with NoOutput

  class DummyViewProvider extends ViewProvider {
    override def createView(createViewInput: CreateViewInput[PersistedView]): Unit = ()
    override def dropView(dropViewInput: DropViewInput): Unit = ()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    DefaultSource.reset()
  }

  test("Rewire nested view after dropping and recreating nested view (bug 104634)") {
    createOrgTable(sqlContext)
    createAnimalsTable(sqlContext)

    sqlContext.sql(s"CREATE VIEW v1 AS SELECT * FROM $orgTbl")
    sqlContext.sql("CREATE VIEW v2 AS SELECT * FROM v1 WHERE ord = 1")

    // drop the view v1.
    sqlContext.catalog.unregisterTable("v1" :: Nil)

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
    sqlContext.catalog.unregisterTable(orgTbl :: Nil)

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
    sqlContext.catalog.unregisterTable("v1" :: Nil)

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

    val actual = sqlContext.catalog.lookupRelation(Seq("v"))

    assertResult(Subquery(
      "v",
      Project(
        UnresolvedAlias(UnresolvedStar(None)) :: Nil,
        UnresolvedRelation(Seq("t")))))(actual)
  }

  test("Valid view provider is issued to create view") {
    val provider = spy(new DummyViewProvider)

    implicit val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOf("qux")).thenReturn(provider)

    val viewCommand =
      CreatePersistentViewCommand(PersistedView(Dummy),
        TableIdentifier("foo"), "qux", Map.empty, allowExisting = true)

    viewCommand.execute(sqlContext)
    verify(provider, times(1)).toSingleViewProvider
    verify(provider, times(1))
      .createView(CreateViewInput(sqlContext, Map.empty, TableIdentifier("foo"),
        PersistedView(Dummy), allowExisting = true))
  }

  test("Upon invalid providers, an exception is thrown") {
    val provider = spy(new DummyViewProvider)

    implicit val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOf("qux")).thenReturn(provider)

    val viewCommand =
      CreatePersistentViewCommand(PersistedDimensionView(Dummy),
        TableIdentifier("foo"), "qux", Map.empty, allowExisting = true)

    intercept[ProviderException] {
      viewCommand.execute(sqlContext)
    }
    verify(provider, times(1)).toSingleViewProvider
  }
}
