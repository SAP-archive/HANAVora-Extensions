package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.types.DataType

/**
  * Base class for all partitioning function creation operations.
  */
sealed abstract class CreatePartitioningFunction
extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty

}

/**
  * Used to represent the operation of create a hash partitioning function.
  *
  * @param parameters The configuration parameters
  * @param name The function name
  * @param provider The provider of the datasource for the function
  * @param datatypes The function datatypes
  * @param partitionsNo The expected number of partitions (optional)
  */
case class CreateHashPartitioningFunction(parameters: Map[String, String],
                                          name: String,
                                          provider: String,
                                          datatypes: Seq[DataType],
                                          partitionsNo: Option[Int])
extends CreatePartitioningFunction


/**
  * Used to represent the operation of create a range partitioning function
  * specified by splitters.
  *
  * @param parameters The configuration parameters
  * @param name The function name
  * @param provider The provider of the datasource for the function
  * @param datatype The function datatype
  * @param splitters The range splitters
  * @param rightClosed (optional) Should be set on true if the ranges are right-closed
  */
case class CreateRangeSplittersPartitioningFunction(parameters: Map[String, String],
                                                    name: String,
                                                    provider: String,
                                                    datatype: DataType,
                                                    splitters: Seq[String],
                                                    rightClosed: Boolean = false)
extends CreatePartitioningFunction

/**
  * Used to represent the operation of create a range partitioning function
  * specified by an interval.
  *
  * @param parameters The configuration parameters
  * @param name The function name
  * @param provider The provider of the datasource for the function
  * @param datatype The function datatype
  * @param start The interval start
  * @param end The interval end
  * @param strideParts Either the stride value ([[Left]]) or parts value ([[Right]])
  */
case class CreateRangeIntervalPartitioningFunction(parameters: Map[String, String],
                                                   name: String,
                                                   provider: String,
                                                   datatype: DataType,
                                                   start: String,
                                                   end: String,
                                                   strideParts: Either[Int, Int])
extends CreatePartitioningFunction
