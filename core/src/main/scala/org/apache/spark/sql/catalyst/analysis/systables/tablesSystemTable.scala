package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.commands.{WithExplicitRelationKind, WithOrigin}
import org.apache.spark.sql.sources._
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
  with ScanAndFilterImplicits {

  /** @inheritdoc */
  override def buildScan(requiredColumns: Array[String],
                         filters: Array[Filter]): RDD[Row] =
    DatasourceResolver
      .resolverFor(sqlContext)
      .newInstanceOfTyped[DatasourceCatalog](provider) match {
      case catalog: DatasourceCatalog with DatasourceCatalogPushDown =>
        catalog.getRelations(sqlContext, options, requiredColumns, filters.toSeq.merge)
      case catalog: DatasourceCatalog =>
        val values =
          catalog
            .getRelations(sqlContext, new CaseInsensitiveMap(options))
            .map(relationInfo => Row(
              relationInfo.name,
              relationInfo.isTemporary.toString.toUpperCase,
              relationInfo.kind.toUpperCase,
              relationInfo.provider))
        val rows = schema.buildPrunedFilteredScan(requiredColumns, filters)(values)
        sparkContext.parallelize(rows)
    }
}

sealed trait TablesSystemTable extends SystemTable {
  override def schema: StructType = TablesSystemTable.schema
}

object TablesSystemTable extends SchemaEnumeration {
  val tableName = Field("TABLE_NAME", StringType, nullable = false)
  val isTemporary = Field("IS_TEMPORARY", StringType, nullable = false)
  val kind = Field("KIND", StringType, nullable = false)
  val provider = Field("PROVIDER", StringType, nullable = true)
}
