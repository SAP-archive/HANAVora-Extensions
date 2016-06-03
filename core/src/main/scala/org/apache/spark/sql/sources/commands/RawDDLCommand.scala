package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Command}
import org.apache.spark.sql.sources.RawDDLObjectType.RawDDLObjectType
import org.apache.spark.sql.sources.RawDDLStatementType.RawDDLStatementType

/**
  * Returned for a DDL commands that can be executed via a RawSqlSourceProvider.
  * Requires the datasource implementing [[org.apache.spark.sql.sources.RawSqlSourceProvider]]
  *
  * @param identifier The parsed identifier of the query
  * @param objectType The type of the database object to be modified
  * @param statementType The type of the DDL statement
  * @param ddlStatement The raw DDL string
  * @param provider The provider class to execute the raw DDL command
  * @param options The query options
  */
private[sql] case class RawDDLCommand(
    identifier: String,
    objectType: RawDDLObjectType,
    statementType: RawDDLStatementType,
    ddlStatement: String,
    provider: String,
    options: Map[String, String])
  extends LeafNode
  with Command {

  override def output: Seq[Attribute] = Seq.empty
}
