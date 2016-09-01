package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.execution.tablefunctions.OutputFormatter
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{DatasourceResolver, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.GenericUtil._

/** A provider of [[PartitionFunctionSystemTable]]s. */
object PartitionFunctionSystemTableProvider extends SystemTableProvider with ProviderBound {

  /** @inheritdoc */
  override def create(sqlContext: SQLContext,
                      provider: String,
                      options: Map[String, String]): SystemTable =
    ProviderBoundPartitionFunctionSystemTable(sqlContext, provider, options)
}

/**
  * A provider bound system table for extracting partition functions from a provider.
  *
  * @param sqlContext The Spark [[SQLContext]].
  * @param provider The provider that implements the [[PartitionCatalog]] interface.
  * @param options The provider specific options.
  */
case class ProviderBoundPartitionFunctionSystemTable(
    sqlContext: SQLContext,
    provider: String,
    options: Map[String, String])
  extends SystemTable
  with AutoScan {

  /** @inheritdoc */
  override def execute(): Seq[Row] = {
    val resolver = DatasourceResolver.resolverFor(sqlContext)
    val catalog = resolver.newInstanceOfTyped[PartitionCatalog](provider)
    catalog.partitionFunctions.flatMap {
      case func@PartitionFunction(id, columns) =>
        val (minP, maxP) = func.matchOptional {
          case MinMaxPartitions(minPartitions, maxPartitions) =>
            (minPartitions, maxPartitions)
        }.getOrElse(None -> None)
        val (blockSize, partitions) = func.matchOptional {
          case b: BlockPartitionFunction => (b.blockSize, b.partitions)
        }.getOrElse(None -> None)
        val boundaries = func.matchOptional {
          case r: RangePartitionFunction => r.boundary
        }.flatten
        val formattedColumns = columns.map {
          case PartitionColumn(columnId, dataType) => Seq(columnId, dataType)
        }
        val typeName = ProviderBoundPartitionFunctionSystemTable.typeNameOf(func)
        new OutputFormatter(
          id, typeName, formattedColumns, boundaries, blockSize, partitions, minP, maxP)
          .format()
          .map(Row.fromSeq)
    }
  }

  /** @inheritdoc */
  override def schema: StructType = PartitionFunctionSystemTable.schema
}

object ProviderBoundPartitionFunctionSystemTable {
  /**
    * Outputs the type name of the given [[PartitionFunction]] for display in the system table.
    *
    * @param f The [[PartitionFunction]].
    * @return The type name of the [[PartitionFunction]].
    */
  private def typeNameOf(f: PartitionFunction): String = f match {
    case _: RangePartitionFunction => "RANGE"
    case _: BlockPartitionFunction => "BLOCK"
    case _: HashPartitionFunction => "HASH"
  }
}

object PartitionFunctionSystemTable extends SchemaEnumeration {
  val id = Field("ID", StringType, nullable = false)
  val functionType = Field("TYPE", StringType, nullable = false)
  val columnName = Field("COLUMN_NAME", StringType, nullable = false)
  val columnType = Field("COLUMN_TYPE", StringType, nullable = false)
  val boundaries = Field("BOUNDARIES", StringType, nullable = true)
  val block = Field("BLOCK_SIZE", IntegerType, nullable = true)
  val partitions = Field("PARTITIONS", IntegerType, nullable = true)
  val minP = Field("MIN_PARTITIONS", IntegerType, nullable = true)
  val maxP = Field("MAX_PARTITIONS", IntegerType, nullable = true)
}
