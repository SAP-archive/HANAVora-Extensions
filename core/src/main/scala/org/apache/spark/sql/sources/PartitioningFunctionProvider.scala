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
                                           splitters: Seq[Int],
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
                                              start: Int,
                                              end: Int,
                                              strideParts: Either[Int, Int]): Unit

  /**
   * Drops an existing partitioning function. If the provided function is referenced by any
   * existing table or it does not exist, the [[RuntimeException]] is thrown. No exception
   * will be thrown if the IF EXIST statement has been used for a non-existing function.
   *
   * @param sqlContext The Spark SQL context
   * @param parameters The configuration parameters
   * @param name Name of the function to create
   * @param allowNotExisting The flag pointing whether an exception should
   *                         be thrown when the function does not exist
   */
  @throws[RuntimeException]("if no function with the provided name and datatypes is defined " +
    "and the IF EXIST statement has not been used")
  @throws[RuntimeException]("if the provided function is referenced by any existing table")
  def dropPartitioningFunction(sqlContext: SQLContext,
                               parameters: Map[String, String],
                               name: String,
                               allowNotExisting: Boolean)

}
