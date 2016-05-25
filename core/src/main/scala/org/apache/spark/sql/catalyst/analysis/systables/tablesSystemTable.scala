package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources.commands.WithExplicitRelationKind
import org.apache.spark.sql.sources.{DatasourceCatalog, TemporaryFlagRelation}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CollectionUtils.CaseInsensitiveMap
import org.apache.spark.sql.{DefaultDatasourceResolver, Row, SQLContext}


object TablesSystemTableProvider extends SystemTableProvider with LocalSpark with ProviderBound {
  override def create(): SystemTable =
    SparkLocalTablesSystemTable

  override def create(provider: String, options: Map[String, String]): SystemTable =
    ProviderBoundTablesSystemTable(provider, options)
}

case object SparkLocalTablesSystemTable extends TablesSystemTable {

  /** @inheritdoc */
  override def execute(sqlContext: SQLContext): Seq[Row] = {
    sqlContext
      .tableNames()
      .map { table =>
        val plan = sqlContext.catalog.lookupRelation(TableIdentifier(table))
        val isTemporary = plan.collectFirst {
          case t: TemporaryFlagRelation => t.isTemporary()
        }.getOrElse(true) // By default, we treat it as temporary
        val kind = plan.collectFirst {
          case r: WithExplicitRelationKind => r.relationKind.name
        }.getOrElse("TABLE") // By default, we treat it as a table
        Row(table, isTemporary.toString.toUpperCase, kind.toUpperCase)
      }
  }
}

/**
  * A system table that lists all the tables in the targeted data source.
  *
  * @param provider The target provider.
  * @param options The options.
  */
case class ProviderBoundTablesSystemTable(
                              provider: String,
                              options: Map[String, String])
  extends TablesSystemTable {

  /** @inheritdoc */
  override def execute(sqlContext: SQLContext): Seq[Row] = {
    val catalog = DefaultDatasourceResolver.newInstanceOf[DatasourceCatalog](provider)

    catalog
      .getRelations(sqlContext, new CaseInsensitiveMap(options))
      .map(relationInfo => Row(
        relationInfo.name,
        relationInfo.isTemporary.toString.toUpperCase,
        relationInfo.kind.toUpperCase))
  }
}

sealed trait TablesSystemTable extends SystemTable {
  override val output: Seq[Attribute] = StructType(
    StructField("TABLE_NAME", StringType, nullable = false) ::
      StructField("IS_TEMPORARY", StringType, nullable = false) ::
      StructField("KIND", StringType, nullable = false) ::
      Nil
  ).toAttributes
}

