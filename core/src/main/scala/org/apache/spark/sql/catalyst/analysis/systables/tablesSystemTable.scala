package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.commands.{WithExplicitRelationKind, WithOrigin}
import org.apache.spark.sql.sources.{DatasourceCatalog, TemporaryFlagRelation}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CollectionUtils.CaseInsensitiveMap
import org.apache.spark.sql.{DatasourceResolver, Row, SQLContext}

object TablesSystemTableProvider extends SystemTableProvider with LocalSpark with ProviderBound {
  /** @inheritdoc */
  override def create(sqlContext: SQLContext): SystemTable =
    SparkLocalTablesSystemTable(sqlContext)

  /** @inheritdoc */
  override def create(sqlContext: SQLContext,
                      provider: String,
                      options: Map[String, String]): SystemTable =
    ProviderBoundTablesSystemTable(sqlContext, provider, options)
}

/**
  * A [[SystemTable]] that lists the spark local catalog items.
  *
  * @param sqlContext The Spark [[SQLContext]].
  */
case class SparkLocalTablesSystemTable(sqlContext: SQLContext)
  extends TablesSystemTable
  with AutoScan {

  /** @inheritdoc */
  override def execute(): Seq[Row] = {
    sqlContext
      .tableNames()
      .map { table =>
        val plan = sqlContext.catalog.lookupRelation(TableIdentifier(table))
        val isTemporary = plan.collectFirst {
          case LogicalRelation(t: TemporaryFlagRelation, _) => t.isTemporary()
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
  * @param sqlContext The Spark [[SQLContext]].
  * @param provider The target provider.
  * @param options The options.
  */
case class ProviderBoundTablesSystemTable(
    sqlContext: SQLContext,
    provider: String,
    options: Map[String, String])
  extends TablesSystemTable
  with AutoScan {

  /** @inheritdoc */
  override def execute(): Seq[Row] = {
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
  override val schema: StructType = StructType(
    StructField("TABLE_NAME", StringType, nullable = false) ::
      StructField("IS_TEMPORARY", StringType, nullable = false) ::
      StructField("KIND", StringType, nullable = false) ::
      StructField("PROVIDER", StringType, nullable = true) :: Nil)
}

