package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.sources.PartitioningFunctionProvider
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.execution.RunnableCommand

/**
 * This command creates a partitioning function according to the provided definition.
 *
 * @param parameters The configuration parameters
 * @param name Name of the function to create
 * @param datatypes Datatypes of the function arguments
 * @param definition Definition of the function to create
 * @param partitionsNo (Optional) the expected number of partitions
 * @param provider The datasource provider (has to implement [[PartitioningFunctionProvider]])
 */
private[sql] case class CreatePartitioningFunctionCommand(parameters: Map[String, String],
                                                          name: String,
                                                          datatypes: Seq[DataType],
                                                          definition: String,
                                                          partitionsNo: Option[Int],
                                                          provider: String)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()

    dataSource match {
      case pfp: PartitioningFunctionProvider =>
        pfp.createPartitioningFunction(sqlContext, parameters, name, datatypes, definition,
          partitionsNo)
        Seq.empty
      case _ => throw new RuntimeException("The provided datasource does not support " +
        "definition of partitioning functions.")
    }
  }

}
