package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.AppendRelation
import org.apache.spark.sql.{Row, SQLContext}

/**
  * @see [[AppendRelation]]
  * @param relation The relation to append to.
  * @param options Options.
  */
private[sql] case class AppendRunnableCommand(
    relation: AppendRelation,
    options: Map[String, String])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    relation.appendFilesToTable(options)
    Seq.empty[Row]
  }
}
