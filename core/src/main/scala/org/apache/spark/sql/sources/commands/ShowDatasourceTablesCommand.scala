package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.types._

/**
 * Returned for the "SHOW DATASOURCETABLES" command.
 */
private[sql] case class ShowDatasourceTablesCommand(
                                                     classIdentifier: String,
                                                     options: Map[String, String])
  extends LogicalPlan
  with Command {

  override def output: Seq[Attribute] = Seq(AttributeReference("tbl_name", StringType,
    nullable = false, new MetadataBuilder()
      .putString("comment", "identifier of the table").build())())

  override def children: Seq[LogicalPlan] = Seq.empty
}
