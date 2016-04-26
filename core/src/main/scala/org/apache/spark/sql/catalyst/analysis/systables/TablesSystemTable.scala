package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources.DatasourceCatalog
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CollectionUtils.CaseInsensitiveMap
import org.apache.spark.sql.{DefaultDatasourceResolver, Row, SQLContext}

/**
  * A system table that lists all the tables in the targeted data source.
  *
  * @param provider The target provider.
  * @param options The options.
  */
case class TablesSystemTable(
                              provider: String,
                              options: Map[String, String])
  extends SystemTable {
  override def execute(sqlContext: SQLContext): Seq[Row] = {
    val catalog = DefaultDatasourceResolver.newInstanceOf[DatasourceCatalog](provider)

    catalog
      .getRelations(sqlContext, new CaseInsensitiveMap(options))
      .map(relationInfo => Row(
        relationInfo.name,
        relationInfo.isTemporary.toString.toUpperCase,
        relationInfo.kind.toUpperCase))
  }

  override val output: Seq[Attribute] = StructType(
    StructField("TABLE_NAME", StringType, nullable = false) ::
      StructField("IS_TEMPORARY", StringType, nullable = false) ::
      StructField("KIND", StringType, nullable = false) ::
      Nil
  ).toAttributes
}

