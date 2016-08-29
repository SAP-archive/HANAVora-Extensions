package org.apache.spark.sql.execution.datasources

import org.apache.spark.Logging
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.DropRelation
import org.apache.spark.sql.{Row, SQLContext}

import scala.util.Try

/**
  * Execution for DROP TABLE command.
  *
  * @see [[DropRelation]]
  */
private[sql] case class DropRunnableCommand(toDrop: Map[String, Option[DropRelation]])
  extends RunnableCommand
  with Logging {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    toDrop.foreach {
      case (name, dropOption) =>
        sqlContext.dropTempTable(name)
        dropOption.foreach { dropRelation =>
          Try {
            dropRelation.dropTable()
          }.recover {
            // When the provider indicates an exception while dropping, we still have to continue
            // dropping all the referencing tables, otherwise there could be integrity issues
            case ex =>
              logWarning(
                s"""Error occurred when dropping table '$name':${ex.getMessage}, however
                   |table '$name' will still be dropped from Spark catalog.
                 """.stripMargin)
          }.get
        }
    }
    Seq.empty
  }
}
