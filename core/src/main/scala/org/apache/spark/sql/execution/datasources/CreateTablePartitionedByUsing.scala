package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.types.StructType

/**
 * Used to represent the operation of create table using a data source
 * with a partitioning function.
 *
 * @param tableIdent The identifier of the table being created
 * @param userSpecifiedSchema The table schema provided by the user
 * @param provider The provider of the datasource for the table
 * @param partitioningFunc The partitioning function definition
 * @param partitioningColumns The partitioning function columns
 * @param temporary If true a temporary table will be created,
 *                  if false a persistent table will be created
 * @param options The table creation options
 * @param allowExisting If it is true, we will do nothing when the table already exists.
 *                      If it is false, an exception will be thrown
 * @param managedIfNoPath Should be set to true, if the table should be managed
 *                        when no path is provided
 */
case class CreateTablePartitionedByUsing(tableIdent: TableIdentifier,
                                         userSpecifiedSchema:
                                         Option[StructType],
                                         provider: String,
                                         partitioningFunc: String,
                                         partitioningColumns: Seq[String],
                                         temporary: Boolean,
                                         options: Map[String, String],
                                         allowExisting: Boolean,
                                         managedIfNoPath: Boolean)
  extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
}
