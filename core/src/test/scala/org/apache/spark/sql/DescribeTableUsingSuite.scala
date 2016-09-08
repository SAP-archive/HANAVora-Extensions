package org.apache.spark.sql

import org.apache.spark.util.SqlContextConfigurationUtils
import org.scalatest.FunSuite
import org.mockito.Mockito._
import DatasourceResolver.withResolver
import org.apache.spark.sql.sources.{DatasourceCatalog, RelationInfo}
import org.scalatest.mock.MockitoSugar

/**
  * Defines a set of tests for
  * [[org.apache.spark.sql.execution.datasources.DescribeTableUsingCommand]]
  */
class DescribeTableUsingSuite
  extends FunSuite
  with SqlContextConfigurationUtils
  with GlobalSapSQLContext
  with MockitoSugar {

  test("DESCRIBE existing table") {
    // simulate existing relation in the data source's catalog.
    com.sap.spark.dstest.DefaultSource.addRelation("t1")
    val actual = sqlContext.sql(s"DESCRIBE TABLE t1 USING com.sap.spark.dstest").collect
    assertResult(Seq(Row("t1", "<DDL statement>")))(actual)
  }

  test("DESCRIBE non-existing table") {
    val actual = sqlContext.sql(s"DESCRIBE TABLE nonExisting USING com.sap.spark.dstest").collect
    assertResult(Seq(Row("", "")))(actual)
  }

  test("DESCRIBE TABLE works with case insensitivity") {
    val provider = mock[DatasourceCatalog]
    when(provider.getRelation(sqlc, Seq("foo"), Map.empty))
      .thenReturn(Some(RelationInfo("foo", isTemporary = false, "TABLE", Some("ddl"), "test")))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[DatasourceCatalog]("test"))
      .thenReturn(provider)

    withResolver(sqlc, resolver) {
      withConf(SQLConf.CASE_SENSITIVE.key, "false") {
        val values = sqlc.sql("DESCRIBE TABLE FOO USING test").collect().toSet
        assertResult(Set(Row("foo", "ddl")))(values)
      }
    }
  }

  test("DESCRIBE TABLE works with case sensitivity") {
    val provider = mock[DatasourceCatalog]
    when(provider.getRelation(sqlc, Seq("FoO"), Map.empty))
      .thenReturn(Some(RelationInfo("foo", isTemporary = false, "TABLE", Some("ddl"), "test")))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[DatasourceCatalog]("test"))
      .thenReturn(provider)

    withResolver(sqlc, resolver) {
      withConf(SQLConf.CASE_SENSITIVE.key, "true") {
        val values = sqlc.sql("DESCRIBE TABLE FoO USING test").collect().toSet
        assertResult(Set(Row("foo", "ddl")))(values)
      }
    }
  }
}
