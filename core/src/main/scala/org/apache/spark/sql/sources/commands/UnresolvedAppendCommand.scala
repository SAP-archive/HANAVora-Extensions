package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, UnaryNode}

/**
 * Returned for the "APPEND TABLE [dbName.]tableName" command.
 * @param table The table where the file is going to be appended
 * @param options The options map with the append configuration
 */
private[sql] case class UnresolvedAppendCommand(
    table: LogicalPlan,
    options: Map[String, String])
  extends UnaryNode
  with Command {

  override lazy val resolved: Boolean = false

  override def child: LogicalPlan = table

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty
}
