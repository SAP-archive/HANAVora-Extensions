package org.apache.spark.sql

import org.scalatest.FunSuite
import org.mockito.Mockito._
import org.mockito.Matchers._
import DatasourceResolver.withResolver
import org.apache.spark.sql.sources.{DatasourceCatalog, RelationInfo}
import org.mockito.internal.stubbing.answers.Returns
import org.scalatest.mock.MockitoSugar

/**
  * Tests for the `SHOW TABLES USING ...` command
  */
class ShowTablesUsingSuite extends FunSuite with MockitoSugar with GlobalSapSQLContext {
  test("SHOW TABLES USING retrieves correct information") {
    val datasource = mock[DatasourceCatalog]
    when(datasource.getRelations(any[SQLContext], any[Map[String, String]]))
      .thenReturn(
        RelationInfo("t1", isTemporary = false, "TABLE", None, "my.custom.resolver") ::
        RelationInfo("t2", isTemporary = true, "TABLE", None, "") ::
        RelationInfo("v1", isTemporary = false, "VIEW", Some("ddl"), "my.custom.resolver") ::
        RelationInfo("v2", isTemporary = true, "VIEW", Some("ddl"), "") :: Nil)
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOf("my.custom.resolver"))
      .thenAnswer(new Returns(datasource))

    withResolver(sqlc, resolver) {
      val values =
        sqlc.sql("SHOW TABLES USING my.custom.resolver").collect().toSet

      assertResult(Set(
        Row("t1", "FALSE", "TABLE"),
        Row("t2", "TRUE", "TABLE"),
        Row("v1", "FALSE", "VIEW"),
        Row("v2", "TRUE", "VIEW")))(values)
    }
  }
}
