package org.apache.spark.sql.execution.systemtables

import org.apache.spark.sql.{GlobalSapSQLContext, Row, SQLContext}
import com.sap.spark.dsmock.DefaultSource._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedSystemTable
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.FunSuite
import SystemTablesSuite._
import org.apache.spark.sql.catalyst.analysis.systables.{SimpleSystemTableRegistry, SystemTable}

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

  test("System table with no parameters") {
    val registry = new SimpleSystemTableRegistry
    registry.register[ZeroArgSystemTable]("zero_arg")

    val resolved = registry.resolve(UnresolvedSystemTable("zero_arg", "test", Map.empty))
    assert(resolved.isInstanceOf[ZeroArgSystemTable])
  }

  test("System table with only option parameters") {
    val registry = new SimpleSystemTableRegistry
    registry.register[OptionsSystemTable]("options_arg")

    val OptionsSystemTable(opts) = registry.resolve(
      UnresolvedSystemTable("options_arg", "test", Map("foo" -> "bar")))

    assertResult(Map("foo" -> "bar"))(opts)
  }

  test("System table with only provider parameter") {
    val registry = new SimpleSystemTableRegistry
    registry.register[ProviderSystemTable]("provider_arg")

    val ProviderSystemTable(provider) = registry.resolve(
      UnresolvedSystemTable("provider_arg", "test", Map.empty))

    assertResult("test")(provider)
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

  class ZeroArgSystemTable extends SystemTable with DummySystemTable

  case class OptionsSystemTable(opts: Map[String, String])
    extends SystemTable
    with DummySystemTable

  case class ProviderSystemTable(provider: String)
    extends SystemTable
    with DummySystemTable
}
