package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.execution.datasources.alterByCatalystSettings
import org.apache.spark.sql.execution.tablefunctions.OutputFormatter
import org.apache.spark.sql.sources.MetadataCatalog
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
  with AutoScan {

  /** @inheritdoc */
  override def execute(): Seq[Row] = {
    val catalog =
      DatasourceResolver
        .resolverFor(sqlContext)
        .newInstanceOfTyped[MetadataCatalog](provider)

    catalog.getTableMetadata(sqlContext, options).flatMap { tableMetadata =>
      val formatter =
        new OutputFormatter(
          alterByCatalystSettings(sqlContext.catalog, tableMetadata.tableName),
          tableMetadata.metadata)
      formatter
        .format()
        .map(Row.fromSeq)
    }
  }

  override val schema: StructType = StructType(
    StructField("TABLE_NAME", StringType, nullable = false) ::
    StructField("METADATA_KEY", StringType, nullable = true) ::
    StructField("METADATA_VALUE", StringType, nullable = true) :: Nil)
}
