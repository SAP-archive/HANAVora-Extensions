package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.sources.commands.RawDDLCommand
import org.apache.spark.sql.sources.{RawSqlSourceProvider}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.execution.RunnableCommand

/**
  * Represents a command that when executed runs the raw DDL string on a datasource that
  * provides a [[RawSqlSourceProvider]].
  *
  * @param ddlStatement The raw DDL string
  */
private[sql] case class RawDDLRunnableCommand(ddlStatement: String)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val dataSource: Any = ResolvedDataSource.lookupDataSource(RawDDLCommand.provider).newInstance()

    dataSource match {
      case rsp: RawSqlSourceProvider =>
        rsp.getRDD(ddlStatement).collect()
      case _ => throw new RuntimeException("The provided datasource does not support " +
        "executing raw DDL statements.")
    }
    Seq.empty[Row]
  }
}
