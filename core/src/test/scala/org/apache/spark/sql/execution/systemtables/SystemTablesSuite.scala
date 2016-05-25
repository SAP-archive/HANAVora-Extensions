package org.apache.spark.sql.execution.systemtables

import org.apache.spark.sql.{GlobalSapSQLContext, Row, SQLContext}
import com.sap.spark.dsmock.DefaultSource._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.FunSuite
import SystemTablesSuite._
import org.apache.spark.sql.catalyst.analysis.systables._

/**
  * Test suites for system tables.
  */
class SystemTablesSuite
  extends FunSuite
  with GlobalSapSQLContext {

  test("Select from TABLES system table and target a datasource") {
    withMock { dataSource =>
      when(dataSource.getRelations(any[SQLContext], any[Map[String, String]]))
        .thenReturn(
          new dataSource.RelationInfo("foo", false, "TABLE", None) ::
          new dataSource.RelationInfo("bar", true, "VIEW", None) :: Nil)

      val values = sqlc.sql("SELECT * FROM SYS.TABLES USING com.sap.spark.dsmock").collect()
      assertResult(Set(
        Row("foo", "FALSE", "TABLE"),
        Row("bar", "TRUE", "VIEW")))(values.toSet)
    }
  }

  test("Select from TABLES system table with local spark as target") {
    sqlc.sql("CREATE TABLE foo(a int, b int) USING com.sap.spark.dstest")
    sqlc.sql("CREATE VIEW bar as SELECT * FROM foo")
    sqlc.sql("CREATE VIEW baz as SELECT * FROM foo USING com.sap.spark.dstest")

    val values = sqlc.sql("SELECT * FROM SYS.TABLES").collect()
    assertResult(Set(
      Row("foo", "TRUE", "TABLE"),
      Row("bar", "TRUE", "VIEW"),
      Row("baz", "FALSE", "VIEW")
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
}

object SystemTablesSuite {

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
