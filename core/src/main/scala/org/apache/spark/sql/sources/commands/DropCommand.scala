package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LeafNode}

/**
 * Returned for the "DROP TABLE [dbName.]tableName" command.
 * @param tableIdentifier The identifier of the table to be dropped
 */
private[sql] case class DropCommand(
    allowNotExisting: Boolean,
    tableIdentifier: TableIdentifier,
    cascade: Boolean)
  extends LeafNode
  with Command {

  override def output: Seq[Attribute] = Seq.empty
}
