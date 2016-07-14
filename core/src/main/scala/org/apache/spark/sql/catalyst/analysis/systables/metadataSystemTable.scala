package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.alterByCatalystSettings
import org.apache.spark.sql.execution.tablefunctions.OutputFormatter
import org.apache.spark.sql.sources.{Filter, MetadataCatalog, MetadataCatalogPushDown}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DatasourceResolver, Row, SQLContext}

object MetadataSystemTableProvider
  extends SystemTableProvider
  with ProviderBound {

  /** @inheritdoc */
  override def create(sqlContext: SQLContext,
                      provider: String,
                      options: Map[String, String]): SystemTable =
    MetadataSystemTable(sqlContext, provider, options)
}

/**
  * A [[SystemTable]] to retrieve technical metadata from a provider related to its tables.
  *
  * @param sqlContext The Spark [[SQLContext]].
  * @param provider The provider that should implement the [[MetadataCatalog]] interface.
  * @param options The provider options.
  */
case class MetadataSystemTable(
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
