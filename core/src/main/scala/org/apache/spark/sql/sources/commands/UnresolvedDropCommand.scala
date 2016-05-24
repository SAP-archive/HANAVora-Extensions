package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LeafNode}

/**
 * Returned for the "DROP TABLE [dbName.]tableName" command.
 * @param relationKind The kind of relation to drop
 * @param allowNotExisting Whether to throw if the targeted relation does not exist.
 * @param tableIdentifier The identifier of the table to be dropped
 * @param cascade True if it should drop related relations. If false and there are
  *               related relations it will throw an exception.
 */
private[sql] case class UnresolvedDropCommand(
    relationKind: RelationKind,
    allowNotExisting: Boolean,
    tableIdentifier: TableIdentifier,
    cascade: Boolean)
  extends LeafNode
  with Command {

  override def output: Seq[Attribute] = Seq.empty
}
