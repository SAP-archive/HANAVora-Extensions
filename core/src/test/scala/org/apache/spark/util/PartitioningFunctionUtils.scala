package org.apache.spark.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.Dissector

/**
  * Utility functions to create and drop various kinds of partitioning functiions.
  */
trait PartitioningFunctionUtils {
  def sqlc: SQLContext

  /**
    * Creates a hash partitioning function in the current [[SQLContext]].
    *
    * @param name The name of the hash partitioning function.
    * @param dataTypes The data types of the partitioning function.
    * @param partitions (optional) The expected number of partitions.
    * @param dataSource The data source that shall be used as a provider.
    */
  def createHashPartitioningFunction(name: String,
                                     dataTypes: Seq[String],
                                     partitions: Option[Int],
                                     dataSource: String): Unit = {
    sqlc.sql(
      s"""
         |CREATE PARTITION FUNCTION $name
         |(${dataTypes.mkString(",")})
         |AS HASH
         |${partitions.map(no => s"PARTITIONS $no").getOrElse("")}
         |USING $dataSource""".stripMargin)
  }

  /**
    * Creates a range partitioning function in the current [[SQLContext]].
    *
    * @param name The name of the partitioning function.
    * @param dataType The data type of the partitioning function.
    * @param start The start of the range.
    * @param end The end of the range.
    * @param interval The [[Dissector]] of the range.
    * @param dataSource The data source that shall be used as a provider.
    */
  def createRangePartitioningFunction(name: String,
                                      dataType: String,
                                      start: Int,
                                      end: Int,
                                      interval: Dissector,
                                      dataSource: String): Unit = {
    sqlc.sql(
      s"""
         |CREATE PARTITION FUNCTION $name
         |($dataType)
         |AS RANGE
         |START $start
         |END $end
         |${intervalToSql(interval)}
         |USING $dataSource
       """.stripMargin
    )
  }

  /**
    * Creates a range split partitioning function in the current [[SQLContext]].
    *
    * @param name The name of the partitioning function.
    * @param dataType The data type of the partitioning function.
    * @param splitters The range splitters' values.
    * @param rightClosed `true` if the range is right closed, `false` otherwise.
    * @param dataSource The data source that shall be used as a provider.
    */
  def createRangeSplitPartitioningFunction(name: String,
                                           dataType: String,
                                           splitters: Seq[Int],
                                           rightClosed: Boolean,
                                           dataSource: String): Unit = {
    sqlc.sql(
      s"""
         |CREATE PARTITION FUNCTION $name
         |($dataType)
         |AS RANGE
         |SPLITTERS
         |${if (rightClosed) "RIGHT CLOSED" else ""}
         |(${splitters.mkString(",")})
         |USING $dataSource
       """.stripMargin
    )
  }

  /**
    * Drops the specified partitioning function from the data source.
    *
    * @param name The name of the partitioning function.
    * @param allowExisting `false` if it should fail if the function does not
    *                      exist, `true` otherwise.
    * @param dataSource The data source to use as provider.
    */
  def dropPartitioningFunction(name: String,
                               allowExisting: Boolean = false,
                               dataSource: String): Unit = {
    sqlc.sql(
      s"""
        |DROP PARTITION FUNCTION
        |${if (allowExisting) "IF EXISTS" else ""}
        |$name
        |USING $dataSource
      """.stripMargin
    )
  }

  private def intervalToSql(interval: Dissector) =
    s"${interval.productPrefix.toUpperCase()} ${interval.n}"
}
