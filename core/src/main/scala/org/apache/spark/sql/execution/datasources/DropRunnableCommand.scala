package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.DropRelation
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Execution for DROP TABLE command.
  *
  * @see [[DropRelation]]
  */
private[sql] case class DropRunnableCommand(toDrop: Map[String, Option[DropRelation]])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    toDrop.foreach {
      case (name, dropOption) =>
        sqlContext.dropTempTable(name)
        dropOption.foreach(_.dropTable())
    }
    Seq.empty
  }
}
