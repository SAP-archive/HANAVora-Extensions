package org.apache.spark.sql.catalyst.analysis.systables
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.tablefunctions._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DatasourceResolver, Row, SQLContext}
import org.apache.spark.sql.catalyst.CaseSensitivityUtils._

import scala.util.Try

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
        val unresolvedPlan = sqlContext.catalog.lookupRelation(TableIdentifier(name))
        // TODO(AC): This should be removed once the new view implementation lands
        Try(sqlContext.analyzer.execute(unresolvedPlan)).map { plan =>
          val extractor = LogicalPlanExtractor(plan)
          extractor.columns.flatMap { column =>
            val nonEmptyAnnotations = OutputFormatter.toNonEmptyMap(column.annotations)
            val formatter =
              new OutputFormatter(
                null,
                sqlContext.fixCase(column.tableName),
                sqlContext.fixCase(column.name),
                sqlContext.fixCase(column.originalTableName),
                sqlContext.fixCase(column.originalName),
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
        }.getOrElse(Seq.empty)
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
  with ScanAndFilterImplicits {

  /** @inheritdoc */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    DatasourceResolver
      .resolverFor(sqlContext)
      .newInstanceOfTyped[DatasourceCatalog](provider) match {
      case catalog: DatasourceCatalog with DatasourceCatalogPushDown =>
        catalog.getSchemas(sqlContext, options, requiredColumns, filters.toSeq.merge)
      case catalog =>
        val values = catalog.getSchemas(sqlContext, options).flatMap {
          case (RelationKey(tableName, originalTableName, schemaOpt), SchemaDescription(fields)) =>
            fields.zipWithIndex.flatMap {
              case (field, index) =>
                val nonEmptyAnnotations =
                  OutputFormatter.toNonEmptyMap(field.metadata)
                val formatter =
                  new OutputFormatter(
                    schemaOpt.orNull,
                    tableName,
                    field.name,
                    originalTableName,
                    field.originalName,
                    index + 1, // Index should start at 1
                    field.nullable,
                    field.typ,
                    field.sparkDataType.map(_.simpleString).orNull,
                    field.numericPrecision.orNull,
                    field.numericPrecisionRadix.orNull,
                    field.numericScale,
                    nonEmptyAnnotations,
                    field.comment.orNull)
                formatter
                  .format()
                  .map(Row.fromSeq)
            }
        }.toSeq
        val rows = schema.buildPrunedFilteredScan(requiredColumns, filters)(values)
        sparkContext.parallelize(rows)
    }
}

/**
  * A base implementation of the schema system table.
  */
sealed trait SchemaSystemTable extends SystemTable {
  override def schema: StructType = SchemaSystemTable.schema
}

object SchemaSystemTable extends SchemaEnumeration {
  val tableSchema = Field("TABLE_SCHEMA", StringType, nullable = true)
  val tableName = Field("TABLE_NAME", StringType, nullable = false)
  val columnName = Field("COLUMN_NAME", StringType, nullable = false)
  val originalTableName = Field("ORIGINAL_TABLE_NAME", StringType, nullable = false)
  val originalColumnName = Field("ORIGINAL_COLUMN_NAME", StringType, nullable = false)
  val ordinalPosition = Field("ORDINAL_POSITION", IntegerType, nullable = false)
  val isNullable = Field("IS_NULLABLE", BooleanType, nullable = false)
  val dataType = Field("DATA_TYPE", StringType, nullable = false)
  val sparkType = Field("SPARK_TYPE", StringType, nullable = true)
  val numericPrecision = Field("NUMERIC_PRECISION", IntegerType, nullable = true)
  val numericPrecisionRadix = Field("NUMERIC_PRECISION_RADIX", IntegerType, nullable = true)
  val numericScale = Field("NUMERIC_SCALE", IntegerType, nullable = true)
  val annotationKey = Field("ANNOTATION_KEY", StringType, nullable = true)
  val annotationValue = Field("ANNOTATION_VALUE", StringType, nullable = true)
  val comment = Field("COMMENT", StringType, nullable = true)
}
