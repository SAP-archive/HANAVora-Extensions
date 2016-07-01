package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.commands.{WithExplicitRelationKind, WithOrigin}
import org.apache.spark.sql.sources.{DatasourceCatalog, TemporaryFlagRelation}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CollectionUtils.CaseInsensitiveMap
import org.apache.spark.sql.{DatasourceResolver, DefaultDatasourceResolver, Row, SQLContext}

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
          case LogicalRelation(r: WithExplicitRelationKind, _) => r.relationKind.name
        }.getOrElse("TABLE") // By default, we treat it as a table
        val origin = plan.collectFirst {
          case o: WithOrigin => o.provider
          case LogicalRelation(o: WithOrigin, _) => o.provider
        }
        Row(table, isTemporary.toString.toUpperCase, kind.toUpperCase, origin.orNull)
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
    val catalog =
      DatasourceResolver
        .resolverFor(sqlContext)
        .newInstanceOfTyped[DatasourceCatalog](provider)

    catalog
      .getRelations(sqlContext, new CaseInsensitiveMap(options))
      .map(relationInfo => Row(
        relationInfo.name,
        relationInfo.isTemporary.toString.toUpperCase,
        relationInfo.kind.toUpperCase,
        provider))
  }
}

sealed trait TablesSystemTable extends SystemTable {
  override val output: Seq[Attribute] = StructType(
    StructField("TABLE_NAME", StringType, nullable = false) ::
      StructField("IS_TEMPORARY", StringType, nullable = false) ::
      StructField("KIND", StringType, nullable = false) ::
      StructField("PROVIDER", StringType, nullable = true) ::
      Nil
  ).toAttributes
}

