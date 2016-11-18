package org.apache.spark.sql.sources.commands.hive

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SQLContext}

/** A dummy command that returns the schema 'default'. */
case object ShowSchemasCommand extends HiveRunnableCommand {

  override protected val commandName: String = "SHOW SCHEMAS"

  override def execute(sqlContext: SQLContext): Seq[Row] =
    Seq(Row("default"))

  override lazy val output: Seq[Attribute] =
    AttributeReference("result", StringType)() :: Nil
}
