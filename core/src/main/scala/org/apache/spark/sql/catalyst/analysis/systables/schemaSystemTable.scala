package org.apache.spark.sql.catalyst.analysis.systables
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.tablefunctions._
import org.apache.spark.sql.sources.commands.Table
import org.apache.spark.sql.sources.{DatasourceCatalog, RelationKey, SchemaDescription, SchemaField}
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.{DatasourceResolver, Row, SQLContext}

/**
  * [[SystemTableProvider]] for the [[SchemaSystemTable]].
  *
  * The [[SchemaSystemTable]] exists for both Spark as [[SparkLocalSchemaSystemTable]] and
  * a provider as [[ProviderBoundSchemaSystemTable]].
  */
object SchemaSystemTableProvider
  extends SystemTableProvider
  with LocalSpark
  with ProviderBound {

  /** @inheritdoc */
  override def create(sqlContext: SQLContext): SystemTable = SparkLocalSchemaSystemTable(sqlContext)

  /** @inheritdoc */
  override def create(sqlContext: SQLContext,
                      provider: String,
                      options: Map[String, String]): SystemTable =
    ProviderBoundSchemaSystemTable(sqlContext, provider, options)

}

/**
  * System table that extracts schemas from local spark.
  *
  * @param sqlContext The Spark [[SQLContext]].
  */
case class SparkLocalSchemaSystemTable(sqlContext: SQLContext)
  extends SchemaSystemTable
  with AutoScan {

  /** @inheritdoc */
  override def execute(): Seq[Row] = {
    sqlContext
      .tableNames()
      .flatMap { name =>
        val tableIdent = TableIdentifier(alterByCatalystSettings(sqlContext.catalog, name))
        val unresolvedPlan = sqlContext.catalog.lookupRelation(tableIdent)
        val plan = sqlContext.analyzer.execute(unresolvedPlan)
        val extractor = new LogicalPlanExtractor(plan)
        extractor.columns.flatMap { column =>
          val nonEmptyAnnotations = OutputFormatter.toNonEmptyMap(column.annotations)
          val formatter =
            new OutputFormatter(
              null,
              column.tableName,
              column.name,
              column.index,
              column.isNullable,
              column.dataType.simpleString,
              column.dataType.simpleString,
              column.numericPrecision.orNull,
              column.numericPrecisionRadix.orNull,
              column.numericScale.orNull,
              nonEmptyAnnotations,
              "" /* columns have empty comment in Spark */)
          formatter
            .format()
            .map(Row.fromSeq)
        }
      }
  }
}

/**
  * A provider bound [[SchemaSystemTable]].
  *
  * @param sqlContext The Spark [[SQLContext]]
  * @param provider The provider implementing [[org.apache.spark.sql.sources.MetadataCatalog]]
  * @param options The options
  */
case class ProviderBoundSchemaSystemTable(
    sqlContext: SQLContext,
    provider: String,
    options: Map[String, String])
  extends SchemaSystemTable
  with AutoScan {

  /** @inheritdoc */
  override def execute(): Seq[Row] = {
    val catalog =
      DatasourceResolver
        .resolverFor(sqlContext)
        .newInstanceOfTyped[DatasourceCatalog](provider)

    catalog.getSchemas(sqlContext, options).flatMap {
      case (RelationKey(tableName, schemaOpt), SchemaDescription(kind, fields)) =>
        val checkStar = kind == Table
        fields.zipWithIndex.flatMap {
          case (SchemaField(name, typ, nullable, sparkTypeOpt, metadata, comment), index) =>
            val annotationsExtractor = new AnnotationsExtractor(metadata, checkStar)
            val nonEmptyAnnotations =
              OutputFormatter.toNonEmptyMap(annotationsExtractor.annotations)
            val dataTypeExtractor = sparkTypeOpt.map(DataTypeExtractor)
            val formatter =
              new OutputFormatter(
                schemaOpt.orNull,
                tableName,
                name,
                index + 1, // Index should start at 1
                nullable,
                typ,
                sparkTypeOpt.map(_.simpleString).orNull,
                dataTypeExtractor.map(_.numericPrecision).orNull,
                dataTypeExtractor.map(_.numericPrecisionRadix).orNull,
                dataTypeExtractor.map(_.numericScale).orNull,
                nonEmptyAnnotations,
                comment)
            formatter
              .format()
              .map(Row.fromSeq)
        }
    }.toSeq
  }
}

/**
  * A base implementation of the schema system table.
  */
sealed trait SchemaSystemTable extends SystemTable {
  protected type TypeHint = Option[String]

  override val schema: StructType = StructType(
    StructField("TABLE_SCHEMA", StringType, nullable = true) ::
    StructField("TABLE_NAME", StringType, nullable = false) ::
    StructField("COLUMN_NAME", StringType, nullable = false) ::
    StructField("ORDINAL_POSITION", IntegerType, nullable = false) ::
    StructField("IS_NULLABLE", BooleanType, nullable = false) ::
    StructField("DATA_TYPE", StringType, nullable = false) ::
    StructField("SPARK_TYPE", StringType, nullable = true) ::
    StructField("NUMERIC_PRECISION", IntegerType, nullable = true) ::
    StructField("NUMERIC_PRECISION_RADIX", IntegerType, nullable = true) ::
    StructField("NUMERIC_SCALE", IntegerType, nullable = true) ::
    StructField("ANNOTATION_KEY", StringType, nullable = true) ::
    StructField("ANNOTATION_VALUE", StringType, nullable = true) ::
    StructField("COMMENT", StringType, nullable = true) :: Nil)
}
