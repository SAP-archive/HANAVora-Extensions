package org.apache.spark.sql.parser

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.commands.hive._

/** A parser to parse some emulated hive commands. */
trait HiveEmulationParser {
  this: SapDQLParser.type =>

  protected lazy val USE = Keyword("USE")
  protected lazy val SHOW = Keyword("SHOW")
  protected lazy val SCHEMAS = Keyword("SCHEMAS")
  protected lazy val DESCRIBE = Keyword("DESCRIBE")
  protected lazy val FORMATTED = Keyword("FORMATTED")

  protected lazy val showSchemas: Parser[ShowSchemasCommand.type] =
    SHOW ~ SCHEMAS ^^^ ShowSchemasCommand

  protected lazy val desc: Parser[DescCommand] =
    (DESCRIBE | DESC) ~> tableIdentifier ^^ DescCommand

  protected lazy val describeFormatted: Parser[DescribeFormattedCommand] =
    DESCRIBE ~> FORMATTED ~> tableIdentifier ^^ DescribeFormattedCommand.apply

  protected lazy val use: Parser[LogicalPlan] =
    USE ~> rep(".*".r) ^^ UseCommand

  protected lazy val hiveEmulationCommands: Parser[LogicalPlan] =
    showSchemas | describeFormatted | desc | use
}
