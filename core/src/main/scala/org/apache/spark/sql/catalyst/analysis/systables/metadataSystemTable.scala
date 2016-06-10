package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.tablefunctions.OutputFormatter
import org.apache.spark.sql.sources.MetadataCatalog
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DefaultDatasourceResolver, Row, SQLContext}
import org.apache.spark.sql.execution.datasources.alterByCatalystSettings

object MetadataSystemTableProvider
  extends SystemTableProvider
  with ProviderBound {

  /** @inheritdoc */
  override def create(provider: String, options: Map[String, String]): SystemTable =
    MetadataSystemTable(provider, options)
}

case class MetadataSystemTable(
    provider: String,
    options: Map[String, String]) extends SystemTable {

  /** @inheritdoc */
  override def execute(sqlContext: SQLContext): Seq[Row] = {
    val catalog = DefaultDatasourceResolver.newInstanceOf[MetadataCatalog](provider)

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

  override val output: Seq[Attribute] = StructType(
    StructField("TABLE_NAME", StringType, nullable = false) ::
      StructField("METADATA_KEY", StringType, nullable = true) ::
      StructField("METADATA_VALUE", StringType, nullable = true) ::
      Nil
  ).toAttributes
}
