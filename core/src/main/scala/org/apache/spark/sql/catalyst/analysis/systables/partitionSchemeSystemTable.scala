package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.sources.PartitionCatalog
import org.apache.spark.sql.{DatasourceResolver, Row, SQLContext}
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}

/**
  * A [[SystemTableProvider]] for [[PartitionSchemeSystemTable]]s.
  */
object PartitionSchemeSystemTableProvider extends SystemTableProvider with ProviderBound {

  /** @inheritdoc */
  override def create(sqlContext: SQLContext,
                      provider: String,
                      options: Map[String, String]): SystemTable =
    ProviderBoundPartitionSchemeSystemTable(sqlContext, provider, options)
}

/**
  * A [[ProviderBound]] system table for [[org.apache.spark.sql.sources.PartitionScheme]]s.
  *
  * @param sqlContext The Spark [[SQLContext]].
  * @param provider The provider that implements the [[PartitionCatalog]] interface.
  * @param options The provider-specific options.
  */
case class ProviderBoundPartitionSchemeSystemTable(
    sqlContext: SQLContext,
    provider: String,
    options: Map[String, String])
  extends SystemTable
  with AutoScan {

  /** @inheritdoc */
  override def execute(): Seq[Row] = {
    val resolver = DatasourceResolver.resolverFor(sqlContext)
    val catalog = resolver.newInstanceOfTyped[PartitionCatalog](provider)
    catalog.partitionSchemes.map { scheme =>
      Row(scheme.id, scheme.partitionFunction.id, scheme.autoColocation)
    }
  }

  /** @inheritdoc */
  override def schema: StructType = PartitionSchemeSystemTable.schema
}

object PartitionSchemeSystemTable extends SchemaEnumeration {
  val id = Field("ID", StringType, nullable = false)
  val partitionFunction = Field("PARTITION_FUNCTION_ID", StringType, nullable = false)
  val colocation = Field("AUTO_COLOCATION", BooleanType, nullable = false)
}
