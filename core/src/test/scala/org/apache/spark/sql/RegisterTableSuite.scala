package org.apache.spark.sql

import org.apache.spark.sql.DatasourceResolver.withResolver
import org.apache.spark.sql.catalyst.CaseSensitivityUtils.DuplicateFieldsException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.DummyRelationUtils._
import org.apache.spark.util.SqlContextConfigurationUtils
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class RegisterTableSuite
  extends FunSuite
  with MockitoSugar
  with GlobalSapSQLContext
  with SqlContextConfigurationUtils {

  test("register table correctly registers a table") {
    val expected = DummyRelation(StructType('a.string))(sqlc)
    val provider = mock[RegisterAllTableRelations]
    when(provider.getTableRelation("foo", sqlc, Map.empty))
      .thenReturn(Some(BaseRelationSource(expected)))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[RegisterAllTableRelations]("bar"))
      .thenReturn(provider)
    withResolver(sqlc, resolver) {
      sqlc.sql("REGISTER TABLE foo USING bar")
      val relation = sqlc.catalog.lookupRelation(TableIdentifier("foo"))
      assert(relation.collectFirst {
        case LogicalRelation(rel, _) if rel eq expected =>
          rel
      }.isDefined)
    }
  }

  test("register table errors in case the target relation does not exist") {
    val provider = mock[RegisterAllTableRelations]
    when(provider.getTableRelation("foo", sqlc, Map.empty)).thenReturn(None)
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[RegisterAllTableRelations]("bar"))
      .thenReturn(provider)
    withResolver(sqlc, resolver) {
      intercept[RuntimeException] {
        sqlc.sql("REGISTER TABLE foo USING bar")
      }
    }
  }

  test("register table errors in case there is already a relation with that " +
    "name in the spark catalog and ignore conflicts is false") {
    sqlContext
      .baseRelationToDataFrame(DummyRelation(StructType(Seq.empty))(sqlc))
      .registerTempTable("foo")

    intercept[RuntimeException] {
      sqlc.sql("REGISTER TABLE foo USING bar")
    }
  }

  test(
    """register table does override if there is already a relation with that name in the
      |spark catalog and ignore conflicts is true.
    """.stripMargin) {
    val expected = DummyRelation(StructType('a.string))(sqlc)
    val provider = mock[RegisterAllTableRelations]
    when(provider.getTableRelation("foo", sqlc, Map.empty))
      .thenReturn(Some(BaseRelationSource(expected)))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[RegisterAllTableRelations]("bar"))
      .thenReturn(provider)
    withResolver(sqlc, resolver) {
      sqlc.sql("REGISTER TABLE foo USING bar IGNORING CONFLICTS")
      val relation = sqlc.catalog.lookupRelation(TableIdentifier("foo"))
      assert(relation.collectFirst {
        case LogicalRelation(rel, _) if rel eq expected =>
          rel
      }.isDefined)
    }
  }

  test("register table throws if the returned source is resolved but the schema returns duplicate" +
    "fields with the current case sensitivity") {
    withConf(SQLConf.CASE_SENSITIVE.key, "false") {
      val originalSchema = StructType('a.string :: 'A.string :: Nil)
      val relation = DummyRelation(originalSchema)(sqlc)
      val provider = mock[RegisterAllTableRelations]
      when(provider.getTableRelation("foo", sqlc, Map.empty))
        .thenReturn(Some(BaseRelationSource(relation)))
      val resolver = mock[DatasourceResolver]
      when(resolver.newInstanceOfTyped[RegisterAllTableRelations]("bar"))
        .thenReturn(provider)
      withResolver(sqlc, resolver) {
        val ex = intercept[RuntimeException] {
          sqlc.sql("REGISTER TABLE foo USING bar")
        }
        assert(ex.getCause.isInstanceOf[DuplicateFieldsException])
        val cause = ex.getCause.asInstanceOf[DuplicateFieldsException]
        assertResult(
          DuplicateFieldsException(
            originalSchema,
            StructType('a.string :: 'a.string :: Nil),
            duplicateFields = Set("a")))(cause)
      }
    }
  }

  test("Register table does not throw if the resolved source is not resolved") {
    val provider = mock[RegisterAllTableRelations]
    val viewHandle = mock[ViewHandle]
    when(provider.getTableRelation("foo", sqlc, Map.empty))
      .thenReturn(
        Some(CreatePersistentViewSource(
          "CREATE VIEW foo AS SELECT a FROM t USING bar",
          viewHandle)))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[RegisterAllTableRelations]("bar"))
      .thenReturn(provider)
    withResolver(sqlc, resolver) {
      sqlc.sql("REGISTER TABLE foo USING bar")
      assert(sqlc.tableNames().contains("foo"))
    }
  }

  test("Register all tables correctly registers all sources") {
    val expected = DummyRelation(StructType('a.string))(sqlc)
    val provider = mock[RegisterAllTableRelations]
    when(provider.getAllTableRelations(sqlc, Map.empty))
      .thenReturn(Map("foo" -> BaseRelationSource(expected), "bar" -> BaseRelationSource(expected)))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[RegisterAllTableRelations]("bar"))
      .thenReturn(provider)
    withResolver(sqlc, resolver) {
      sqlc.sql("REGISTER ALL TABLEs USING bar")
      assertResult(Set("foo", "bar"))(sqlc.tableNames().toSet)
    }
  }

  test("Register all tables errors if case insensitive and if there are table names only " +
    "differing in case") {
    withConf(SQLConf.CASE_SENSITIVE.key, "false") {
      val expected = DummyRelation(StructType('a.string))(sqlc)
      val provider = mock[RegisterAllTableRelations]
      when(provider.getAllTableRelations(sqlc, Map.empty))
        .thenReturn(
          Map("foo" -> BaseRelationSource(expected), "Foo" -> BaseRelationSource(expected)))
      val resolver = mock[DatasourceResolver]
      when(resolver.newInstanceOfTyped[RegisterAllTableRelations]("bar"))
        .thenReturn(provider)
      withResolver(sqlc, resolver) {
        intercept[RuntimeException] {
          sqlc.sql("REGISTER ALL TABLES USING bar")
        }
      }
    }
  }

  test("Register all tables errors if there are conflicting spark local and retrieved tables") {
    val expected = DummyRelation(StructType('a.string))(sqlc)
    val provider = mock[RegisterAllTableRelations]
    when(provider.getAllTableRelations(sqlc, Map.empty))
      .thenReturn(Map("foo" -> BaseRelationSource(expected), "bar" -> BaseRelationSource(expected)))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[RegisterAllTableRelations]("bar"))
      .thenReturn(provider)
    withResolver(sqlc, resolver) {
      sqlc.baseRelationToDataFrame(expected).registerTempTable("foo")
      intercept[RuntimeException] {
        sqlc.sql("REGISTER ALL TABLES USING bar")
      }
    }
  }

  test("Register all tables does not error if there are existing tables " +
    "but ignore conflicts is set") {
    val expected = DummyRelation(StructType('a.string))(sqlc)
    val provider = mock[RegisterAllTableRelations]
    when(provider.getAllTableRelations(sqlc, Map.empty))
      .thenReturn(Map("foo" -> BaseRelationSource(expected), "bar" -> BaseRelationSource(expected)))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[RegisterAllTableRelations]("bar"))
      .thenReturn(provider)
    withResolver(sqlc, resolver) {
      sqlc.baseRelationToDataFrame(expected).registerTempTable("foo")
      sqlc.sql("REGISTER ALL TABLES USING bar IGNORING CONFLICTS")
      assertResult(Set("foo", "bar"))(sqlc.tableNames().toSet)
    }
  }

  test("Register all tables does not error if case insensitive and if there are table names only " +
    "differing in case but ignore conflicts is set") {
    withConf(SQLConf.CASE_SENSITIVE.key, "false") {
      val expected = DummyRelation(StructType('a.string))(sqlc)
      val provider = mock[RegisterAllTableRelations]
      when(provider.getAllTableRelations(sqlc, Map.empty))
        .thenReturn(
          Map("foo" -> BaseRelationSource(expected), "Foo" -> BaseRelationSource(expected)))
      val resolver = mock[DatasourceResolver]
      when(resolver.newInstanceOfTyped[RegisterAllTableRelations]("bar"))
        .thenReturn(provider)
      withResolver(sqlc, resolver) {
        sqlc.sql("REGISTER ALL TABLES USING bar IGNORING CONFLICTS")
        assertResult(Set("foo"))(sqlc.tableNames().toSet)
      }
    }
  }

  test("REGISTER ALL TABLES IF NOT EXISTS does not error or overwrite" +
    "if there are existing tables") {
    val expected = DummyRelation(StructType('a.string))(sqlc)
    val registeredRelation = DummyRelation(StructType(Array('a.int, 'b.int)))(sqlc)
    val provider = mock[RegisterAllTableRelations]
    when(provider.getAllTableRelations(sqlc, Map.empty))
      .thenReturn(Map("foo" -> BaseRelationSource(expected), "bar" -> BaseRelationSource(expected)))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[RegisterAllTableRelations]("bar"))
      .thenReturn(provider)
    withResolver(sqlc, resolver) {
      // existing relation foo
      sqlc.baseRelationToDataFrame(registeredRelation).registerTempTable("foo")
      sqlc.sql("REGISTER ALL TABLES IF NOT EXISTS USING bar")
      assertResult(Set("foo", "bar"))(sqlc.tableNames().toSet)
      // check if schema of 'foo' is still the same
      assertResult(registeredRelation.schema)(sqlc.sql("SELECT * FROM foo").schema)
    }
  }

  test("REGISTER ALL TABLES IGNORING CONFLICTS does not error bug overwrites existing tables") {
    val expected = DummyRelation(StructType('a.string))(sqlc)
    val registeredRelation = DummyRelation(StructType(Array('a.int, 'b.int)))(sqlc)
    val provider = mock[RegisterAllTableRelations]
    when(provider.getAllTableRelations(sqlc, Map.empty))
      .thenReturn(Map("foo" -> BaseRelationSource(expected), "bar" -> BaseRelationSource(expected)))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[RegisterAllTableRelations]("bar"))
      .thenReturn(provider)
    withResolver(sqlc, resolver) {
      // existing relation foo
      sqlc.baseRelationToDataFrame(registeredRelation).registerTempTable("foo")
      sqlc.sql("REGISTER ALL TABLES USING bar IGNORING CONFLICTS")
      assertResult(Set("foo", "bar"))(sqlc.tableNames().toSet)
      // check if schema of 'foo' is still the same
      assertResult(expected.schema)(sqlc.sql("SELECT * FROM foo").schema)
    }
  }
}
