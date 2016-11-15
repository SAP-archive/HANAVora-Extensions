package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LeafNode}
import org.apache.spark.sql.execution.datasources.DropTarget

/** Returned for the "DROP TABLE [dbName.]tableName" command. */
sealed trait UnresolvedDropCommand
  extends LeafNode
  with Command {

  /** The [[DropTarget]] of the relation to drop. */
  val target: DropTarget

  /**
    * `true` if it should ignore not existing relations,
    * `false` otherwise, which will throw if the targeted relation does not exist.
    */
  val ifExists: Boolean

  /** The [[TableIdentifier]] of the table to be dropped. */
  val tableIdentifier: TableIdentifier

  /**
    * `true` if it should drop related relations,
    * `false` otherwise, which will throw if related relations exist.
    */
  val cascade: Boolean

  override def output: Seq[Attribute] = Seq.empty
}

/**
  * Returned for the "DROP TABLE [dbName.]tableName" command.
  * @param target The target type of relation to drop
  * @param ifExists Whether to throw if the targeted relation does not exist.
  * @param tableIdentifier The identifier of the table to be dropped
  * @param cascade True if it should drop related relations. If false and there are
  *               related relations it will throw an exception.
  */
private[sql] case class UnresolvedSparkLocalDropCommand(
    target: DropTarget,
    ifExists: Boolean,
    tableIdentifier: TableIdentifier,
    cascade: Boolean)
  extends UnresolvedDropCommand

/**
  * Returned for the "DROP TABLE [dbName.]tableName" command.
  * @param target The [[DropTarget]] of the relation to drop.
  * @param ifExists `true` if it should ignore not existing relations,
  *                 `false` otherwise, which will throw if the targeted relation does not exist.
  * @param tableIdentifier The [[TableIdentifier]] of the table to be dropped.
  * @param cascade `true` if it should drop related relations,
  *                `false` otherwise, which will throw if related relations exist.
  * @param options A [[Map]] which contains the options.
  * @param provider The provider to be used.
  */
private[sql] case class UnresolvedProviderBoundDropCommand(
    target: DropTarget,
    ifExists: Boolean,
    tableIdentifier: TableIdentifier,
    cascade: Boolean,
    options: Map[String, String],
    provider: String)
  extends UnresolvedDropCommand
