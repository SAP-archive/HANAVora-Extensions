package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataType

/**
 * This trait contains a method which should be implemented by
 * providers of partitioning functions for Spark relations.
 */
trait PartitioningFunctionProvider {

  /**
   * Creates a new partitioning function according to the provided
   * definition. If a function is already defined, the [[RuntimeException]]
   * is thrown.
   *
   * @param sqlContext The Spark SQL context
   * @param parameters The configuration parameters
   * @param name Name of the function to create
   * @param datatypes Datatypes of the function arguments
   * @param definition Definition of the function to create
   * @param partitionsNo (Optional) the expected number of partitions
   */
  @throws[RuntimeException]("if there is already a function with the same name defined")
  def createPartitioningFunction(sqlContext: SQLContext,
                                 parameters: Map[String, String],
                                 name: String,
                                 datatypes: Seq[DataType],
                                 definition: String,
                                 partitionsNo: Option[Int]): Unit

}
