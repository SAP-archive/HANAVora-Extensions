package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.types.DataType

/**
 * Used to represent the operation of create a partitioning function.
 *
 * @param parameters The configuration parameters
 * @param name The function name
 * @param datatypes The function datatypes
 * @param definition The function definition passed by the user
 * @param partitionsNo The expected number of partitions (optional)
 * @param provider The provider of the datasource for the function
 */
case class CreatePartitioningFunction(parameters: Map[String, String],
                                      name: String,
                                      datatypes: Seq[DataType],
                                      definition: String,
                                      partitionsNo: Option[Int],
                                      provider: String)
extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty

}
