package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataType

/**
 * This trait contains a method which should be implemented by
 * providers of partitioning functions for Spark relations.
 */
trait PartitioningFunctionProvider {

  /**
   * Creates a new hash partitioning function according to the provided
   * definition. If the function is already defined, the [[RuntimeException]]
   * is thrown.
   *
   * @param sqlContext The Spark SQL context
   * @param parameters The configuration parameters
   * @param name Name of the function to create
   * @param datatypes Datatypes of the function arguments
   * @param partitionsNo (Optional) the expected number of partitions
   */
  @throws[RuntimeException]("if there is already a function with the same name defined")
  def createHashPartitioningFunction(sqlContext: SQLContext,
                                     parameters: Map[String, String],
                                     name: String,
                                     datatypes: Seq[DataType],
                                     partitionsNo: Option[Int]): Unit

  /**
    * Creates a new range partitioning function based on a set of splitters,
    * according to the provided definition. If the function is already defined,
    * the [[RuntimeException]] is thrown.
    *
    * @param sqlContext The Spark SQL context
    * @param parameters The configuration parameters
    * @param name Name of the function to create
    * @param datatype The function argument's datatype
    * @param splitters The range splitters
    * @param rightClosed Should be set on true if the ranges are right-closed
    */
  @throws[RuntimeException]("if there is already a function with the same name defined")
  def createRangeSplitPartitioningFunction(sqlContext: SQLContext,
                                           parameters: Map[String, String],
                                           name: String,
                                           datatype: DataType,
                                           splitters: Seq[String],
                                           rightClosed: Boolean): Unit

  /**
    * Creates a new range partitioning function based on the provided interval
    * and a stride/parts value, according to the provided definition. If the function
    * is already defined, the [[RuntimeException]] is thrown.
    *
    * @param sqlContext The Spark SQL context
    * @param parameters The configuration parameters
    * @param name Name of the function to create
    * @param datatype The function argument's datatype
    * @param start The interval start
    * @param end The interval end
    * @param strideParts Either the stride value ([[Left]]) or parts value ([[Right]])
    */
  @throws[RuntimeException]("if there is already a function with the same name defined")
  def createRangeIntervalPartitioningFunction(sqlContext: SQLContext,
                                              parameters: Map[String, String],
                                              name: String,
                                              datatype: DataType,
                                              start: String,
                                              end: String,
                                              strideParts: Either[Int, Int]): Unit

}
