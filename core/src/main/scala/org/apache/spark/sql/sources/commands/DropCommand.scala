package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}

/**
 * Returned for the "DROP TABLE [dbName.]tableName" command.
 * @param table The table to be dropped
 */
private[sql] case class DropCommand(
                                     allowNotExisting: Boolean,
                                     table: UnresolvedRelation,
                                     cascade: Boolean)
  extends LogicalPlan
  with Command {

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty
}
