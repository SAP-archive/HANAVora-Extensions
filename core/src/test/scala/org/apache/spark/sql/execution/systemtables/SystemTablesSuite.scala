package org.apache.spark.sql.execution.systemtables

import org.apache.spark.sql.{GlobalSapSQLContext, Row, SQLContext}
import com.sap.spark.dsmock.DefaultSource._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.FunSuite
import SystemTablesSuite._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.systables.DependenciesSystemTable.ReferenceDependency
import org.apache.spark.sql.catalyst.analysis.systables._
import org.apache.spark.sql.execution.datasources.SqlContextAccessor._
import org.apache.spark.sql.sources.TableMetadata

/**
  * Test suites for system tables.
  */
class SystemTablesSuite
  extends FunSuite
  with GlobalSapSQLContext {

  test("Select from TABLE_METADATA system table returns table metadata") {
    withMock { dataSource =>
      when(dataSource.getTableMetadata(any[SQLContext], any[Map[String, String]]))
        .thenReturn(Seq(TableMetadata("foo", Map("bar" -> "baz", "qux" -> "bang"))))

      val values =
        sqlc
          .sql("SELECT * FROM SYS.TABLE_METADATA USING com.sap.spark.dsmock")
          .collect()
          .toSet

      assertResult(Set(Row("foo", "bar", "baz"), Row("foo", "qux", "bang")))(values)
    }
  }

  test("Select from TABLES system table and target a datasource") {
    withMock { dataSource =>
      when(dataSource.getRelations(any[SQLContext], any[Map[String, String]]))
        .thenReturn(
          new dataSource.RelationInfo("foo", false, "TABLE", None) ::
          new dataSource.RelationInfo("bar", true, "VIEW", None) :: Nil)

      val values = sqlc.sql("SELECT * FROM SYS.TABLES USING com.sap.spark.dsmock").collect()
      assertResult(Set(
        Row("foo", "FALSE", "TABLE", "com.sap.spark.dsmock"),
        Row("bar", "TRUE", "VIEW", "com.sap.spark.dsmock")))(values.toSet)
    }
  }

  test("Select from TABLES system table with local spark as target") {
    sqlc.sql("CREATE TABLE foo(a int, b int) USING com.sap.spark.dstest")
    sqlc.sql("CREATE VIEW bar as SELECT * FROM foo")
    sqlc.sql("CREATE VIEW baz as SELECT * FROM foo USING com.sap.spark.dstest")

    val values = sqlc.sql("SELECT * FROM SYS.TABLES").collect()
    assertResult(Set(
      Row("foo", "TRUE", "TABLE", "com.sap.spark.dstest"),
      Row("bar", "TRUE", "VIEW", null),
      Row("baz", "FALSE", "VIEW", "com.sap.spark.dstest")
    ))(values.toSet)
  }

  test("Resolution of non existing system tables throws an exception") {
    val registry = new SimpleSystemTableRegistry

    intercept[SystemTableException.NotFoundException] {
      registry.resolve(UnresolvedSparkLocalSystemTable("foo"))
    }
  }

  test("Lookup of spark bound system table provider works case insensitive") {
    val registry = new SimpleSystemTableRegistry
    registry.register("foo", DummySystemTableProvider)

    assertResult(Some(DummySystemTableProvider))(registry.lookup("foo"))
    assertResult(Some(DummySystemTableProvider))(registry.lookup("FOO"))
  }

  test("Resolution of spark system table works") {
    val registry = new SimpleSystemTableRegistry
    registry.register("foo", DummySystemTableProvider)

    val resolved = registry.resolve(UnresolvedSparkLocalSystemTable("foo"))
    assertResult(SparkSystemTable)(resolved)
  }

  test("Resolution of provider bound system table works") {
    val registry = new SimpleSystemTableRegistry
    registry.register("foo", DummySystemTableProvider)

    val provider = "bar"
    val options = Map("a" -> "b", "c" -> "d")
    val resolved = registry.resolve(UnresolvedProviderBoundSystemTable("foo", provider, options))
    assert(resolved.isInstanceOf[ProviderSystemTable])
    val sysTable = resolved.asInstanceOf[ProviderSystemTable]
    assertResult(options)(sysTable.options)
    assertResult(provider)(sysTable.provider)
  }

  test("Resolution fails if the provider does not support the given spark local table") {
    val registry = new SimpleSystemTableRegistry
    registry.register("foo", new SystemTableProvider with ProviderBound {
      override def create(provider: String, options: Map[String, String]): SystemTable =
        throw new Exception
    })

    intercept[SystemTableException.InvalidProviderException] {
      registry.resolve(UnresolvedSparkLocalSystemTable("foo"))
    }
  }

  test("Resolution fails if the provider does not support the given provider bound table") {
    val registry = new SimpleSystemTableRegistry
    registry.register("foo", new SystemTableProvider with LocalSpark {
      override def create(): SystemTable = throw new Exception
    })

    intercept[SystemTableException.InvalidProviderException] {
      registry.resolve(UnresolvedProviderBoundSystemTable("foo", "bar", Map.empty))
    }
  }

  test("dependencies system table produces correct results") {
    sqlc.registerRawPlan(DummyPlan, "table")
    sqlc.registerRawPlan(NonPersistedView(UnresolvedRelation(TableIdentifier("table"))), "view")

    val Array(dependencies) = sqlc.sql("SELECT * FROM SYS.OBJECT_DEPENDENCIES").collect()
    assertResult(
      Row(
        null,
        "table",
        "TABLE",
        null,
        "view",
        "VIEW",
        ReferenceDependency.id))(dependencies)
  }

  test("Projection of system tables works correctly (bug 114354)") {
    withMock { dataSource =>
      when(dataSource.getRelations(any[SQLContext], any[Map[String, String]]))
        .thenReturn(
          new dataSource.RelationInfo("t1", false, "TABLE", None) ::
            new dataSource.RelationInfo("t2", true, "TABLE", None) ::
            new dataSource.RelationInfo("v1", true, "VIEW", None) ::
            new dataSource.RelationInfo("v2", true, "VIEW", None) :: Nil)

      val result1 = sqlc
        .sql("SELECT TABLE_NAME FROM SYS.TABLES USING com.sap.spark.dsmock").collect()
      assertResult(Set(Row("t1"), Row("t2"), Row("v1"), Row("v2")))(result1.toSet)
      val result2 = sqlc
        .sql("SELECT TABLE_NAME, KIND FROM SYS.TABLES using com.sap.spark.dsmock").collect()
      assertResult(Set(Row("t1", "TABLE"), Row("t2", "TABLE"),
        Row("v1", "VIEW"), Row("v2", "VIEW")))(result2.toSet)
      val result3 = sqlc
        .sql("SELECT TABLE_NAME AS test, KIND FROM SYS.TABLES USING com.sap.spark.dsmock").collect()
      assertResult(Set(Row("t1", "TABLE"), Row("t2", "TABLE"),
        Row("v1", "VIEW"), Row("v2", "VIEW")))(result3.toSet)
      val result4 = sqlc
        .sql("SELECT * FROM SYS.TABLES USING com.sap.spark.dsmock WHERE TABLE_NAME LIKE \"t1\"")
        .collect()
      assertResult(Set(Row("t1", "FALSE", "TABLE", "com.sap.spark.dsmock")))(result4.toSet)
      val result5 = sqlc
        .sql("SELECT * FROM SYS.TABLES USING com.sap.spark.dsmock LIMIT 1").collect()
      assertResult(Set(Row("t1", "FALSE", "TABLE", "com.sap.spark.dsmock")))(result5.toSet)
    }
  }

}

object SystemTablesSuite {
  case object DummyPlan extends LeafNode {
    override def output: Seq[Attribute] = Seq.empty
  }

  trait DummySystemTable extends SystemTable {
    override def execute(sqlContext: SQLContext): Seq[Row] = Seq.empty

    override def output: Seq[Attribute] = Seq.empty

    override def productElement(n: Int): Any = ()

    override def productArity: Int = 0

    override def canEqual(that: Any): Boolean = false
  }

  object SparkSystemTable extends SystemTable with DummySystemTable

  case class ProviderSystemTable(provider: String, options: Map[String, String])
    extends SystemTable with DummySystemTable

  object DummySystemTableProvider
    extends SystemTableProvider
    with LocalSpark
    with ProviderBound {

    override def create(): SystemTable = SparkSystemTable

    override def create(provider: String, options: Map[String, String]): SystemTable =
      ProviderSystemTable(provider, options)
  }
}
