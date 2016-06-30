package org.apache.spark.sql.execution.systemtables

import com.sap.spark.dsmock.DefaultSource._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.systables._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.SqlContextAccessor._
import org.apache.spark.sql.execution.systemtables.SystemTablesSuite._
import org.apache.spark.sql.sources.commands.Table
import org.apache.spark.sql.sources.{RelationKey, SchemaDescription, SchemaField, TableMetadata}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{GlobalSapSQLContext, Row, SQLContext}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite

/**
  * Test suites for system tables.
  */
class SystemTablesSuite
  extends FunSuite
  with GlobalSapSQLContext {

  override def beforeEach(): Unit = {
    super.beforeEach()
    sqlc.catalog.unregisterAllTables()
  }

  val schema = StructType(
    StructField(
      "foo",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("meta", "data").build()) ::
    StructField(
      "bar",
      IntegerType,
      nullable = true) :: Nil
  )

  private def toSchemaField(structField: StructField,
                            customType: String,
                            customComment: String): SchemaField =
    SchemaField(
      structField.name,
      customType,
      structField.nullable,
      Some(structField.dataType),
      MetadataAccessor.metadataToMap(structField.metadata),
      Some(customComment))

  val schemaFields = schema.map { field =>
    field.dataType match {
      case StringType => toSchemaField(field, "CUSTOM-String", "CUSTOM-COMMENT1")
      case IntegerType => toSchemaField(field, "CUSTOM-Integer", "CUSTOM-COMMENT2")
    }
  }

  test("SELECT from SCHEMAS system table with local spark is also aware of views") {
    val table = DummyTable(schema)
    sqlc.registerRawPlan(table, "tab")
    sqlc.sql(
      """CREATE VIEW v AS
        |SELECT foo as vFoo @ (meta = 'morph'),
        |bar as vBar @ (meta = 'data')
        |FROM tab""".stripMargin)

    val values = sqlc.sql("SELECT * FROM SYS.SCHEMAS").collect()
    // scalastyle:off magic.number
    assertResult(Set(
      Row(null, "tab", "foo", 1, false, "string", "string", null, null, null, "meta", "data", ""),
      Row(null, "tab", "bar", 2, true, "int", "int", 32, 2, 0, null, null, ""),
      Row(null, "tab", "vFoo", 1, false, "string", "string", null, null, null, "meta", "morph", ""),
      Row(null, "tab", "vBar", 2, true, "int", "int", 32, 2, 0, "meta", "data", "")
    ))(values.toSet)
    // scalastyle:on magic.number
  }

  test("SELECT from SCHEMAS system table with local spark returns spark catalog data") {
    val table = DummyTable(schema)
    sqlc.registerRawPlan(table, "tab")

    val values = sqlc.sql("SELECT * FROM SYS.SCHEMAS").collect()
    // scalastyle:off magic.number
    assertResult(Set(
      Row(null, "tab", "foo", 1, false, "string", "string", null, null, null, "meta", "data", ""),
      Row(null, "tab", "bar", 2, true, "int", "int", 32, 2, 0, null, null, "")
    ))(values.toSet)
    // scalastyle:on magic.number
  }

  test("SELECT from SCHEMAS system table with target provider returns formatted data") {
    withMock { dataSource =>
      when(dataSource.getSchemas(any[SQLContext], any[Map[String, String]]))
        .thenReturn(Map(RelationKey("tab") -> SchemaDescription(Table, schemaFields)))

      val values = sqlc.sql("SELECT * FROM SYS.SCHEMAS USING com.sap.spark.dsmock").collect()
      // scalastyle:off magic.number
      assertResult(Set(
        Row(null, "tab", "foo", 1, false, "CUSTOM-String", "string", null, null, null,
          "meta", "data", "CUSTOM-COMMENT1"),
        Row(null, "tab", "bar", 2, true, "CUSTOM-Integer", "int", 32, 2, 0, null, null,
          "CUSTOM-COMMENT2")
      ))(values.toSet)
      // scalastyle:on magic.number
    }
  }

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
      registry.resolve(UnresolvedSparkLocalSystemTable("foo"), sqlContext)
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

    val resolved = registry.resolve(UnresolvedSparkLocalSystemTable("foo"), sqlContext)
    assertResult(SparkSystemTable(sqlContext))(resolved)
  }

  test("Resolution of provider bound system table works") {
    val registry = new SimpleSystemTableRegistry
    registry.register("foo", DummySystemTableProvider)

    val provider = "bar"
    val options = Map("a" -> "b", "c" -> "d")
    val resolved =
      registry.resolve(UnresolvedProviderBoundSystemTable("foo", provider, options), sqlContext)
    assert(resolved.isInstanceOf[ProviderSystemTable])
    val sysTable = resolved.asInstanceOf[ProviderSystemTable]
    assertResult(options)(sysTable.options)
    assertResult(provider)(sysTable.provider)
  }

  test("Resolution fails if the provider does not support the given spark local table") {
    val registry = new SimpleSystemTableRegistry
    registry.register("foo", new SystemTableProvider with ProviderBound {
      override def create(sqlContext: SQLContext,
                          provider: String,
                          options: Map[String, String]): SystemTable =
        throw new Exception
    })

    intercept[SystemTableException.InvalidProviderException] {
      registry.resolve(UnresolvedSparkLocalSystemTable("foo"), sqlContext)
    }
  }

  test("Resolution fails if the provider does not support the given provider bound table") {
    val registry = new SimpleSystemTableRegistry
    registry.register("foo", new SystemTableProvider with LocalSpark {
      override def create(sqlContext: SQLContext): SystemTable = throw new Exception
    })

    intercept[SystemTableException.InvalidProviderException] {
      registry.resolve(UnresolvedProviderBoundSystemTable("foo", "bar", Map.empty), sqlContext)
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

  test("Column scans and filters work on system tables") {
    withMock { dataSource =>
      when(dataSource.getRelations(any[SQLContext], any[Map[String, String]]))
        .thenReturn(
          new dataSource.RelationInfo("t1", false, "TABLE", None) ::
            new dataSource.RelationInfo("t2", true, "TABLE", None) ::
            new dataSource.RelationInfo("v1", true, "VIEW", None) ::
            new dataSource.RelationInfo("v2", true, "VIEW", None) :: Nil)

      val results = sqlc.sql(
        """SELECT TABLE_NAME FROM SYS.TABLES
          |USING com.sap.spark.dsmock
          |WHERE TABLE_NAME LIKE '%1'
          |AND TABLE_NAME LIKE 't%'
          |AND KIND = 'TABLE'
        """.stripMargin).collect().toList

      assertResult(List(Row("t1")))(results)
    }
  }

}

object SystemTablesSuite {
  case class DummyTable(structType: StructType) extends LeafNode {
    override val output: Seq[Attribute] = structType.toAttributes
  }

  case object DummyPlan extends LeafNode {
    override def output: Seq[Attribute] = Seq.empty
  }

  trait DummySystemTable extends SystemTable with AutoScan {
    override def execute(): Seq[Row] = Seq.empty

    override def schema: StructType = StructType(Seq.empty)
  }

  case class SparkSystemTable(sqlContext: SQLContext) extends SystemTable with DummySystemTable

  case class ProviderSystemTable(
      sqlContext: SQLContext,
      provider: String,
      options: Map[String, String])
    extends SystemTable with DummySystemTable

  object DummySystemTableProvider
    extends SystemTableProvider
    with LocalSpark
    with ProviderBound {

    override def create(sqlContext: SQLContext): SystemTable = SparkSystemTable(sqlContext)

    override def create(sqlContext: SQLContext,
                        provider: String,
                        options: Map[String, String]): SystemTable =
      ProviderSystemTable(sqlContext, provider, options)
  }
}
