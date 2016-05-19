package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Command}

/**
  * Returned for a DDL command for strings containing 'USING com.sap.spark.vora.engines'.
  * Requires the datasource implementing [[org.apache.spark.sql.sources.RawSqlSourceProvider]]
  *
  * @param ddlStatement The raw DDL string
  */
private[sql] case class RawDDLCommand(var ddlStatement: String = "")
  extends LeafNode
  with Command {

  override def output: Seq[Attribute] = Seq.empty
}

// this command can for now only run against com.sap.spark.engines data source
private[sql] object RawDDLCommand {
  val provider: String = "com.sap.spark.engines"
}
