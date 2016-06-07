package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.sources.PartitioningFunctionProvider
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.execution.RunnableCommand

/**
  * Base class for partitioning function creation commands.
  */
private[sql] sealed abstract class CreatePartitioningFunctionCommand extends RunnableCommand

/**
 * This command creates a hash partitioning function according to the provided arguments.
 *
 * @param parameters The configuration parameters
 * @param name Name of the function to create
 * @param datatypes Datatypes of the function arguments
 * @param partitionsNo (Optional) the expected number of partitions
 * @param provider The datasource provider (has to implement [[PartitioningFunctionProvider]])
 */
private[sql] case class CreateHashPartitioningFunctionCommand(parameters: Map[String, String],
                                                              name: String,
                                                              datatypes: Seq[DataType],
                                                              partitionsNo: Option[Int],
                                                              provider: String)
  extends CreatePartitioningFunctionCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val functionId = alterByCatalystSettings(sqlContext.catalog, name)
    val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()

    dataSource match {
      case pfp: PartitioningFunctionProvider =>
        pfp.createHashPartitioningFunction(sqlContext, parameters, functionId, datatypes,
          partitionsNo)
        Seq.empty
      case _ => throw new RuntimeException("The provided datasource does not support " +
        "definition of partitioning functions.")
    }
  }

}

/**
  * This command creates a range-split partitioning function according to the provided arguments.
  *
  * @param parameters The configuration parameters
  * @param name Name of the function to create
  * @param datatype The function argument's datatype
  * @param splitters The range splitters
  * @param rightClosed (optional) Should be set on true if the ranges are right-closed
  * @param provider The datasource provider (has to implement [[PartitioningFunctionProvider]])
  */
private[sql] case class CreateRangeSplitPartitioningFunctionCommand(parameters: Map[String, String],
                                                                    name: String,
                                                                    datatype: DataType,
                                                                    splitters: Seq[Int],
                                                                    rightClosed: Boolean,
                                                                    provider: String)
  extends CreatePartitioningFunctionCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val functionId = alterByCatalystSettings(sqlContext.catalog, name)
    val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()

    dataSource match {
      case pfp: PartitioningFunctionProvider =>
        pfp.createRangeSplitPartitioningFunction(sqlContext, parameters, functionId, datatype,
          splitters, rightClosed)
        Seq.empty
      case _ => throw new RuntimeException("The provided datasource does not support " +
        "definition of partitioning functions.")
    }
  }

}

/**
  * This command creates a range-interval partitioning function according to the provided
  * definition.
  *
  * @param parameters The configuration parameters
  * @param name Name of the function to create
  * @param datatype The function argument's datatype
  * @param start The interval start
  * @param end The interval end
  * @param strideParts Either the stride value ([[Left]]) or parts value ([[Right]])
  * @param provider The datasource provider (has to implement [[PartitioningFunctionProvider]])
  */
private[sql] case class CreateRangeIntervalPartitioningFunctionCommand
(parameters: Map[String, String], name: String, datatype: DataType,
 start: Int, end: Int, strideParts: Either[Int, Int], provider: String)
  extends CreatePartitioningFunctionCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val functionId = alterByCatalystSettings(sqlContext.catalog, name)
    val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()

    dataSource match {
      case pfp: PartitioningFunctionProvider =>
        pfp.createRangeIntervalPartitioningFunction(sqlContext, parameters, functionId,
          datatype, start, end, strideParts)
        Seq.empty
      case _ => throw new RuntimeException("The provided datasource does not support " +
        "definition of partitioning functions.")
    }
  }

}

/**
 * This command drops a partitioning function with the provided definition.
 *
 * @param parameters The configuration parameters
 * @param name The function name
 * @param allowNotExisting The flag pointing whether an exception should
 *                         be thrown when the function does not exist
 * @param provider The datasource provider (has to implement [[PartitioningFunctionProvider]])
 */
private[sql] case class DropPartitioningFunctionCommand
(parameters: Map[String, String], name: String, allowNotExisting: Boolean, provider: String)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val functionId = alterByCatalystSettings(sqlContext.catalog, name)
    val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()

    dataSource match {
      case pfp: PartitioningFunctionProvider =>
        pfp.dropPartitioningFunction(sqlContext, parameters, functionId, allowNotExisting)
        Seq.empty
      case _ => throw new RuntimeException("The provided datasource does not support " +
        "definition of partitioning functions.")
    }
  }

}
