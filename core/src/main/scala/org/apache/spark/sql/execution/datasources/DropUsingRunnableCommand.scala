package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.TableIdentifierUtils._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.DropProvider
import org.apache.spark.sql.{Row, SQLContext}


/**
  * Execution for DROP TABLE command.
  *
  * @param provider The [[DropProvider]] to be used.
  * @param target The [[DropTarget]] of the relation to drop.
  * @param ifExists `true` if it should ignore not existing relations,
  *                 `false` otherwise, which will throw if the targeted relation does not exist.
  * @param tableIdentifier The [[TableIdentifier]] of the table to be dropped.
  * @param cascade `true` if it should drop related relations,
  *                `false` otherwise, which will throw if related relations exist.
  * @param options A [[Map]] which contains the options.
  */
private[sql]
case class DropUsingRunnableCommand(
    provider: DropProvider,
    target: DropTarget,
    ifExists: Boolean,
    tableIdentifier: TableIdentifier,
    cascade: Boolean,
    options: Map[String, String])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    provider.dropRelation(sqlContext, target, ifExists, tableIdentifier.toSeq, cascade, options)
    Seq.empty
  }
}
