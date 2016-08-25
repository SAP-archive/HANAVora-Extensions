package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.tablefunctions.OutputFormatter
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DatasourceResolver, Row, SQLContext}
import org.apache.spark.sql.catalyst.CaseSensitivityUtils._

/** A provider for the metadata system table. */
object MetadataSystemTableProvider
  extends SystemTableProvider
  with LocalSpark
  with ProviderBound {


  /** @inheritdoc */
  override def create(sqlContext: SQLContext): SystemTable =
    SparkLocalMetadataSystemTable(sqlContext)


  /** @inheritdoc */
  override def create(sqlContext: SQLContext,
                      provider: String,
                      options: Map[String, String]): SystemTable =
    ProviderBoundMetadataSystemTable(sqlContext, provider, options)
}

/**
  * The spark local metadata system table.
  *
  * Technical metadata is retrieved from [[Relation]]s that implement
  * the [[MetadataRelation]] trait. For non-relations, `null` is shown
  * for both key and value.
  *
  * @param sqlContext The Spark [[SQLContext]].
  */
case class SparkLocalMetadataSystemTable(sqlContext: SQLContext)
  extends SystemTable
  with AutoScan {

  /** @inheritdoc */
  override def execute(): Seq[Row] = {
    sqlContext.tableNames.flatMap { name =>
      val plan = sqlContext.catalog.lookupRelation(TableIdentifier(name))
      val metadataRelationOpt = Relation.unapply(plan).collect { case m: MetadataRelation => m }
      val metadata = metadataRelationOpt.fold(Map.empty[String, String])(_.metadata)
      val nonEmptyMetadata = if (metadata.isEmpty) Map((null, null)) else metadata
      nonEmptyMetadata.map {
        case (key, value) =>
          Row(sqlContext.fixCase(name), key, value)
      }
    }
  }

  override def schema: StructType = MetadataSystemTable.schema
}

/**
  * A [[SystemTable]] to retrieve technical metadata from a provider related to its tables.
  *
  * @param sqlContext The Spark [[SQLContext]].
  * @param provider The provider that should implement the [[MetadataCatalog]] interface.
  * @param options The provider options.
  */
case class ProviderBoundMetadataSystemTable(
    sqlContext: SQLContext,
    provider: String,
    options: Map[String, String])
  extends SystemTable
  with ScanAndFilterImplicits {

  /** @inheritdoc */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] =
    DatasourceResolver
      .resolverFor(sqlContext)
      .newInstanceOfTyped[MetadataCatalog](provider) match {
      case catalog: MetadataCatalog with MetadataCatalogPushDown =>
        catalog.getTableMetadata(sqlContext, options, requiredColumns, filters.toSeq.merge)
      case catalog =>
        val rows = catalog.getTableMetadata(sqlContext, options).flatMap { tableMetadata =>
          val formatter = new OutputFormatter(tableMetadata.tableName, tableMetadata.metadata)
          formatter.format().map(Row.fromSeq)
        }
        sparkContext.parallelize(schema.buildPrunedFilteredScan(requiredColumns, filters)(rows))
    }

  override def schema: StructType = MetadataSystemTable.schema
}

object MetadataSystemTable extends SchemaEnumeration {
  val tableName = Field("TABLE_NAME", StringType, nullable = false)
  val metadataKey = Field("METADATA_KEY", StringType, nullable = true)
  val metadataValue = Field("METADATA_VALUE", StringType, nullable = true)
}
