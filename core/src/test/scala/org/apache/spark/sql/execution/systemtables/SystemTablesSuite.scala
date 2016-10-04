package org.apache.spark.sql.execution.systemtables

import com.sap.spark.dsmock.DefaultSource._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.systables._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.SqlContextAccessor._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.apache.spark.sql.DatasourceResolver._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.plans.logical.view.NonPersistedPlainView
import org.apache.spark.sql.execution.tablefunctions.DataTypeExtractor
import org.apache.spark.sql.execution.systemtables.SystemTablesSuite._
import org.mockito.internal.stubbing.answers.Returns
import org.scalatest.mock.MockitoSugar
import org.apache.spark.util.DummyRelationUtils._
import org.apache.spark.util.SqlContextConfigurationUtils

/**
  * Test suites for system tables.
  */
class SystemTablesSuite
  extends FunSuite
  with GlobalSapSQLContext
  with MockitoSugar
  with SqlContextConfigurationUtils {

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
                            customComment: String): SchemaField = {
    val dataTypeExtractor = DataTypeExtractor(structField.dataType)
    SchemaField(
      structField.name,
      structField.name,
      customType,
      structField.nullable,
      Some(structField.dataType),
      dataTypeExtractor.numericPrecision,
      dataTypeExtractor.numericPrecisionRadix,
      dataTypeExtractor.numericScale,
      MetadataAccessor.metadataToMap(structField.metadata).mapValues(_.toString),
      Some(customComment))
  }

  private def assertSameResultRegardlessOfCaseSensitivity(query: String)
                                                         (expected: Set[Row]): Unit = {
    def runQuery(): Set[Row] = sqlc.sql(query).collect().toSet
    val caseInsensitiveValues = withConf(SQLConf.CASE_SENSITIVE.key, "false")(runQuery())
    val caseSensitiveValues = withConf(SQLConf.CASE_SENSITIVE.key, "true")(runQuery())

    assertResult(expected)(caseSensitiveValues)
    assertResult(caseSensitiveValues)(caseInsensitiveValues)
  }

  val schemaFields = schema.map { field =>
    field.dataType match {
      case StringType => toSchemaField(field, "CUSTOM-String", "CUSTOM-COMMENT1")
      case IntegerType => toSchemaField(field, "CUSTOM-Integer", "CUSTOM-COMMENT2")
    }
  }

  test("Schema enumeration keeps the order of elements") {
    object TestEnumeration extends SchemaEnumeration {
      val name = Field("name", StringType)
      val age = Field("age", IntegerType)
      val address = Field("address", StringType)
    }

    assertResult(
      StructType(
        StructField("name", StringType) ::
        StructField("age", IntegerType) ::
        StructField("address", StringType) :: Nil))(TestEnumeration.schema)
  }

  test("Schema enumeration errors if the schema is accessed and then another field is added") {
    object TestEnumeration extends SchemaEnumeration {
      schema
      intercept[IllegalStateException] {
        Field("test", StringType)
      }
    }

    assertResult(StructType(Nil))(TestEnumeration.schema)
  }

  test("Schema enumeration errors if fields with identical names are defined") {
    object TestEnumeration extends SchemaEnumeration {
      val name = Field("name", StringType)
      intercept[IllegalStateException] {
        Field("name", IntegerType)
      }
    }

    assertResult(TestEnumeration.schema)(StructType(StructField("name", StringType) :: Nil))
  }

  test("Pushdown enabled datasource catalog for schema systable is preferred over normal one") {
    abstract class PushDownDatasourceCatalog
      extends DatasourceCatalog
        with DatasourceCatalogPushDown
    val pushDownDatasourceCatalog = mock[PushDownDatasourceCatalog]
    when(
      pushDownDatasourceCatalog.getSchemas(
        any[SQLContext],
        any[Map[String, String]],
        any[Seq[String]],
        any[Option[sources.Filter]]))
      .thenReturn(sc.parallelize(Seq(Row("t"))))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[DatasourceCatalog]("com.sap.provider"))
      .thenAnswer(new Returns(pushDownDatasourceCatalog))

    withResolver(sqlContext, resolver) {
      val values =
        sqlc.sql("SELECT TABLE_NAME FROM SYS.SCHEMAS USING com.sap.provider")
          .collect()
          .toList
      assertResult(List(Row("t")))(values)
    }
  }

  test("Pushdown enabled datasource catalog for tables systable is preferred over normal one") {
    abstract class PushDownDatasourceCatalog
      extends DatasourceCatalog
      with DatasourceCatalogPushDown
    val pushDownDatasourceCatalog = mock[PushDownDatasourceCatalog]
    when(
      pushDownDatasourceCatalog.getRelations(
        any[SQLContext],
        any[Map[String, String]],
        any[Seq[String]],
        any[Option[sources.Filter]]))
      .thenReturn(sc.parallelize(Seq(Row("t"))))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[DatasourceCatalog]("com.sap.provider"))
      .thenAnswer(new Returns(pushDownDatasourceCatalog))

    withResolver(sqlContext, resolver) {
      val values =
        sqlc.sql("SELECT TABLE_NAME FROM SYS.TABLES USING com.sap.provider")
          .collect()
          .toList
      assertResult(List(Row("t")))(values)
    }
  }

  test("Pushdown enabled metadata catalog is preferred over normal one") {
    abstract class PushDownMetadataCatalog extends MetadataCatalog with MetadataCatalogPushDown
    val pushDownMetadataCatalog = mock[PushDownMetadataCatalog]
    when(
      pushDownMetadataCatalog.getTableMetadata(
        any[SQLContext],
        any[Map[String, String]],
        any[Seq[String]],
        any[Option[sources.Filter]])).thenReturn(sc.parallelize(Seq(Row("t", "foo", "bar"))))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[MetadataCatalog]("com.sap.provider"))
      .thenAnswer(new Returns(pushDownMetadataCatalog))

    withResolver(sqlContext, resolver) {
      val values =
        sqlc.sql("SELECT * FROM SYS.TABLE_METADATA USING com.sap.provider").collect().toList
      assertResult(List(Row("t", "foo", "bar")))(values)
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
      Row(null, "tab", "foo", "tab", "foo", 1, false,
        "string", "string", null, null, null, "meta", "data", ""),
      Row(null, "tab", "bar", "tab", "bar", 2, true,
        "int", "int", 32, 2, 0, null, null, ""),
      Row(null, "v", "vFoo", "tab", "foo", 1, false,
        "string", "string", null, null, null, "meta", "morph", ""),
      Row(null, "v", "vBar", "tab", "bar", 2, true,
        "int", "int", 32, 2, 0, "meta", "data", "")
    ))(values.toSet)
    // scalastyle:on magic.number
  }

  test("SELECT from SCHEMAS system table with local spark returns spark catalog data") {
    val table = DummyTable(schema)
    sqlc.registerRawPlan(table, "tab")

    val values = sqlc.sql("SELECT * FROM SYS.SCHEMAS").collect()
    // scalastyle:off magic.number
    assertResult(Set(
      Row(null, "tab", "foo", "tab", "foo", 1, false,
        "string", "string", null, null, null, "meta", "data", ""),
      Row(null, "tab", "bar", "tab", "bar", 2, true,
        "int", "int", 32, 2, 0, null, null, "")
    ))(values.toSet)
    // scalastyle:on magic.number
  }

  test("SELECT from SCHEMAS system table with target provider returns formatted data") {
    withMock { dataSource =>
      when(dataSource.getSchemas(any[SQLContext], any[Map[String, String]]))
        .thenReturn(Map(RelationKey("tab", "oTab") -> SchemaDescription(schemaFields)))

      val values = sqlc.sql("SELECT * FROM SYS.SCHEMAS USING com.sap.spark.dsmock").collect()
      // scalastyle:off magic.number
      assertResult(Set(
        Row(null, "tab", "foo", "oTab", "foo", 1, false,
          "CUSTOM-String", "string", null, null, null, "meta", "data", "CUSTOM-COMMENT1"),
        Row(null, "tab", "bar", "oTab", "bar", 2, true,
          "CUSTOM-Integer", "int", 32, 2, 0, null, null, "CUSTOM-COMMENT2")
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

  test("Select from spark local TABLE_METADATA system table returns metadata of local tables") {
    abstract class MockMetadataRelation extends UnaryNode with Table with MetadataRelation

    val nonMetadataRelation = mock[LogicalPlan]
    val metadataRelation1 = mock[MockMetadataRelation]
    val metadataRelation2 = mock[MockMetadataRelation]
    val metadataRelation3 = mock[MockMetadataRelation]

    when(nonMetadataRelation.collectFirst[Relation](any[Nothing])).thenReturn(None)
    Seq(metadataRelation1, metadataRelation2, metadataRelation3).foreach { metadataRelation =>
      when(metadataRelation.collectFirst[Relation](any[PartialFunction[LogicalPlan, Relation]]))
        .thenReturn(Some(metadataRelation))
    }

    when(metadataRelation1.metadata).thenReturn(Map("foo" -> "bar", "baz" -> "bang"))
    when(metadataRelation2.metadata).thenReturn(Map("baz" -> "1"))
    when(metadataRelation3.metadata).thenReturn(Map.empty[String, String])

    sqlContext.registerRawPlan(nonMetadataRelation, "nometadata")
    sqlContext.registerRawPlan(metadataRelation1, "metadata1")
    sqlContext.registerRawPlan(metadataRelation2, "metadata2")
    sqlContext.registerRawPlan(metadataRelation3, "metadata3")

    val values =
      sqlContext
        .sql("SELECT TABLE_NAME, METADATA_KEY, METADATA_VALUE FROM SYS.TABLE_METADATA")
        .collect()
        .toSet

    assertResult(
      Set(
        Row("nometadata", null, null),
        Row("metadata1", "foo", "bar"),
        Row("metadata1", "baz", "bang"),
        Row("metadata2", "baz", "1"),
        Row("metadata3", null, null)))(values)
  }

  test("Select from TABLES system table returns the correct result set (Bug 117362, 117363)") {
    withMock { dataSource =>
      val handle = mock[ViewHandle]
      when(dataSource.createView(any[CreateViewInput])).thenReturn(handle)
      when(dataSource.createRelation(
        sqlContext = any[SQLContext],
        tableName = any[Seq[String]],
        parameters = any[Map[String, String]],
        schema = any[StructType],
        partitioningFunction = any[Option[String]],
        partitioningColumns = any[Option[Seq[String]]],
        isTemporary = any[Boolean],
        allowExisting = any[Boolean]))
        .thenReturn(new DummyRelation('foo.string)(sqlc) with Table {
          override def isTemporary: Boolean = false
        })

      sqlc.sql("CREATE TABLE t (foo string) USING com.sap.spark.dsmock")
      sqlc.sql("CREATE VIEW v1 AS SELECT * FROM t USING com.sap.spark.dsmock")
      sqlc.sql("CREATE VIEW v2 AS SELECT * FROM t")

      val values = sqlc.sql("SELECT TABLE_NAME, KIND, IS_TEMPORARY FROM SYS.TABLES").collect().toSet
      assertResult(
        Set(
          Row("t", "TABLE", "FALSE"),
          Row("v1", "VIEW", "FALSE"),
          Row("v2", "VIEW", "TRUE")))(values)
    }
  }

  test("Select from TABLES system table and target a datasource") {
    withMock { dataSource =>
      when(dataSource.getRelations(any[SQLContext], any[Map[String, String]]))
        .thenReturn(
          new RelationInfo("foo", false, "TABLE", None, "com.sap.spark.dsmock") ::
          new RelationInfo("bar", true, "VIEW", None, "com.sap.spark.dsmock") :: Nil)

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
      Row("foo", "FALSE", "TABLE", "com.sap.spark.dstest"),
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
    sqlc.registerRawPlan(
      NonPersistedPlainView(UnresolvedRelation(TableIdentifier("table"))), "view")

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
          new RelationInfo("t1", false, "TABLE", None, "com.sap.spark.dsmock") ::
            new RelationInfo("t2", true, "TABLE", None, "com.sap.spark.dsmock") ::
            new RelationInfo("v1", true, "VIEW", None, "com.sap.spark.dsmock") ::
            new RelationInfo("v2", true, "VIEW", None, "com.sap.spark.dsmock") :: Nil)

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
          new RelationInfo("t1", false, "TABLE", None, "com.sap.spark.dsmock") ::
            new RelationInfo("t2", true, "TABLE", None, "com.sap.spark.dsmock") ::
            new RelationInfo("v1", true, "VIEW", None, "com.sap.spark.dsmock") ::
            new RelationInfo("v2", true, "VIEW", None, "com.sap.spark.dsmock") :: Nil)

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

  test("Tables system table returns correct temporary information (Bug 117362)") {
    sqlc.baseRelationToDataFrame(DummyRelation('a.string)(sqlc)).registerTempTable("t1")
    sqlc.baseRelationToDataFrame(new DummyRelation('a.string)(sqlc) with Table {
      override def isTemporary: Boolean = false
    }).registerTempTable("t2")
    sqlc.sql("CREATE VIEW v1 AS SELECT * FROM foo USING com.sap.spark.dstest")
    sqlc.sql("CREATE VIEW v2 AS SELECT * FROM v1")
    val values =
      sqlc.sql("SELECT TABLE_NAME, IS_TEMPORARY FROM SYS.TABLES").collect.map(_.toSeq).toSet
    assertResult(Set(Seq("t1", "TRUE"), Seq("t2", "FALSE"), Seq("v1", "FALSE"), Seq("v2", "TRUE")))(
      values)
  }

  test("Select from session system table") {
    try {
      sqlContext.setConf("foo", "bar")
      sqlContext.sparkContext.conf.set("baz", "qux")

      val result = sqlc.sql("SELECT SECTION, KEY, VALUE FROM SYS.SESSION_CONTEXT").collect().toSet

      assert(result.contains(Row("SparkContext", "baz", "qux")))
      assert(result.contains(Row("SQLContext", "foo", "bar")))
    } finally {
      sqlContext.conf.unsetConf("foo")
      sqlContext.sparkContext.conf.remove("baz")
    }
  }

  test("Select from partition scheme system table") {
    val catalog = mock[PartitionCatalog]
    when(catalog.partitionSchemes)
      .thenReturn(
        Seq(
          PartitionScheme(
            "PS1",
            BlockPartitionFunction("PF1", Seq.empty, 1, 0),
            autoColocation = false),
          PartitionScheme(
            "PS2",
            BlockPartitionFunction("PF1", Seq.empty, 1, 0),
          autoColocation = true)))
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[PartitionCatalog]("foo"))
      .thenReturn(catalog)

    withResolver(sqlContext, resolver) {
      val values =
        sqlc.sql(
          """SELECT ID, PARTITION_FUNCTION_ID, AUTO_COLOCATION
            |FROM SYS.PARTITION_SCHEMES
            |USING foo""".stripMargin)
          .collect()
          .toSet

      assertResult(
        Set(
          Row("PS1", "PF1", false),
          Row("PS2", "PF1", true)))(values)
    }
  }

  test("Select from partition function system table") {
    val catalog = mock[PartitionCatalog]
    when(catalog.partitionFunctions)
      .thenReturn(
        Seq(
          RangePartitionFunction(
            id = "a",
            columns = Seq(
              PartitionColumn("c1", "string"),
              PartitionColumn("c2", "int")),
            boundary = Some("boundary"),
            minPartitions = Some(1),
            maxPartitions = None),
          BlockPartitionFunction(
            id = "b",
            columns = Seq(
              PartitionColumn("c1", "float")),
            blockSize = 1,
            partitions = 0),
          HashPartitionFunction(
            id = "c",
            columns = Seq(
              PartitionColumn("c1", "timestamp")),
            minPartitions = None,
            maxPartitions = Some(1))
        )
      )
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[PartitionCatalog]("foo"))
      .thenReturn(catalog)

    withResolver(sqlContext, resolver) {
      val values =
        sqlc.sql(
          """SELECT
            |  ID, TYPE, COLUMN_NAME, COLUMN_TYPE,
            |  BOUNDARIES, BLOCK_SIZE, PARTITIONS,
            |  MIN_PARTITIONS, MAX_PARTITIONS
            |FROM SYS.PARTITION_FUNCTIONS
            |USING foo""".stripMargin)
          .collect()
          .toSet

      assertResult(
        Set(
          Row("a", "RANGE", "c1", "string", "boundary", null, null, 1, null),
          Row("a", "RANGE", "c2", "int", "boundary", null, null, 1, null),
          Row("b", "BLOCK", "c1", "float", null, 1, 0, null, null),
          Row("c", "HASH", "c1", "timestamp", null, null, null, null, 1)))(values)
    }
  }

  test("Select from TABLE_METADATA system table preserves casing of everything") {
    withMock { dataSource =>
      when(dataSource.getTableMetadata(any[SQLContext], any[Map[String, String]]))
        .thenReturn(Seq(TableMetadata("Foo", Map("bar" -> "baZ", "qUx" -> "bang"))))

      assertSameResultRegardlessOfCaseSensitivity(
        "SELECT * FROM SYS.TABLE_METADATA USING com.sap.spark.dsmock")(
        Set(Row("Foo", "bar", "baZ"), Row("Foo", "qUx", "bang")))
    }
  }

  test("SELECT from RELATION_SQL_NAME system table returns correct mappings") {
    val rows = sc.parallelize(Seq(Row("foo"), Row("bar")))
    sqlContext.createDataFrame(rows, StructType('a.string :: Nil)).registerTempTable("t1")
    sqlContext.catalog.registerTable(
      TableIdentifier("t2"),
      LogicalRelation(SqlLikeDummyRelation("sqlName", 'a.string)(sqlc)))

    val values =
      sqlContext
        .sql("SELECT RELATION_NAME, SQL_NAME FROM SYS.RELATION_SQL_NAME")
        .collect()
        .toSet

    assertResult(Set(Row("t1", null), Row("t2", "sqlName")))(values)
  }

  test("SELECT FROM TABLES system table preserves casing") {
    withMock { dataSource =>
      val relations = Seq("Foo", "baR", "baz").map { name =>
        RelationInfo(name, isTemporary = false, "TABLE", None, "com.sap.spark.dsmock")
      }
      when(dataSource.getRelations(sqlContext, Map.empty))
        .thenReturn(relations)

      assertSameResultRegardlessOfCaseSensitivity(
        "SELECT TABLE_NAME FROM SYS.TABLES USING com.sap.spark.dsmock")(
        Set(Row("Foo"), Row("baR"), Row("baz")))
    }
  }

  test("SELECT FROM SCHEMAS system table preserves casing") {
    withMock { dataSource =>
      val names = "Foo" -> "fOo" :: "Bar" -> "baR" :: Nil
      val keys = names.map {
        case (name, originalName) => RelationKey(name, originalName)
      }
      val schema = SchemaDescription(Seq(toSchemaField('a.string, "foo", "bar")))
      val map: Map[RelationKey, SchemaDescription] = keys.map { key =>
        key -> schema
      }(collection.breakOut)

      when(dataSource.getSchemas(sqlContext, Map.empty))
        .thenReturn(map)

      assertSameResultRegardlessOfCaseSensitivity(
        "SELECT TABLE_NAME, ORIGINAL_TABLE_NAME FROM SYS.SCHEMAS USING com.sap.spark.dsmock")(
        Set(Row("Foo", "fOo"), Row("Bar", "baR")))
    }
  }

  test("SELECT FROM OBJECT_DEPENDENCIES works in case of invalid views") {
    sqlc.sql("CREATE VIEW v AS SELECT * FROM t")

    val values =
      sqlc.sql("""SELECT BASE_OBJECT_NAME, BASE_OBJECT_TYPE,
                |        DEPENDENT_OBJECT_NAME, DEPENDENT_OBJECT_TYPE
                |FROM SYS.OBJECT_DEPENDENCIES
                |WHERE DEPENDENT_OBJECT_NAME = 'v'""".stripMargin).collect.toSet

    assertResult(Set(Row("t", "UNKNOWN", "v", "VIEW")))(values)
  }

  test("SELECT FROM SYS.TABLES returns correct view types") {
    sqlc.sql("CREATE TABLE t (a int, b int) USING com.sap.spark.dstest")
    sqlc.sql("CREATE DIMENSION VIEW v1 AS SELECT * FROM t")
    sqlc.sql("CREATE CUBE VIEW v2 AS SELECT * FROM t")
    sqlc.sql("CREATE VIEW v3 AS SELECT * FROM t")

    val values = sqlc.sql("SELECT TABLE_NAME, KIND FROM SYS.TABLES").collect().toSet

    assertResult(Set(
      Row("t", "TABLE"),
      Row("v1", "DIMENSION"),
      Row("v2", "CUBE"),
      Row("v3", "VIEW")))(values)
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
