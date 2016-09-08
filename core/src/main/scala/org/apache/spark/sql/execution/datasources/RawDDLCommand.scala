package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.sources.RawDDLObjectType.RawDDLObjectType
import org.apache.spark.sql.sources.RawDDLStatementType.RawDDLStatementType
import org.apache.spark.sql.sources.{RawSqlSourceProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.execution.RunnableCommand

/**
  * Represents a command that when executed runs the raw DDL string on a datasource that
  * provides a [[RawSqlSourceProvider]].
  *
  * @param identifier The identifier modified by this DDL statement
  * @param objectType The database object type modified by this DDL statement
  * @param statementType The tyep of the DDL statement
  * @param sparkSchema The schema of the columns to create, optional
  * @param ddlStatement The raw DDL string
  * @param provider The provider for execution
  * @param options The options to be sent to the provider
  */
private[sql] case class RawDDLCommand(
    identifier: String,
    objectType: RawDDLObjectType,
    statementType: RawDDLStatementType,
    sparkSchema: Option[StructType],
    ddlStatement: String,
    provider: String,
    options: Map[String, String])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()

    dataSource match {
      case rsp: RawSqlSourceProvider =>
        rsp.executeDDL(identifier, objectType, statementType, sparkSchema, ddlStatement, options)
      case _ => throw new RuntimeException("The provided datasource does not support " +
        "executing raw DDL statements.")
    }
    Seq.empty[Row]
  }
}
