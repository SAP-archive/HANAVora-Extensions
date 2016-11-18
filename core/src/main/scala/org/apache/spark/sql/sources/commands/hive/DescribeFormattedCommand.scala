package org.apache.spark.sql.sources.commands.hive

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.commands.hive.DescribeFormattedCommand._
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * An emulated hive command that describes a relation in form of a tabular string.
  *
  * @param ident The name of the relation to describe.
  */
case class DescribeFormattedCommand(ident: TableIdentifier) extends HiveRunnableCommand {

  override protected def commandName: String = s"DESCRIBE $ident"

  override def execute(sqlContext: SQLContext): Seq[Row] = {
    val plan = sqlContext.catalog.lookupRelation(ident)

    val lines = columnInformation(plan)

    lines.map(Row.apply(_))
  }

  private def columnInformation(plan: LogicalPlan): Seq[String] = {
    val header = headerize(tabularize("col_name", "data_type", "comment"))
    val fields: Seq[StructField] = if (plan.resolved) plan.schema.fields else Seq.empty

    header +: "" +: fields.map {
      case StructField(name, dataType, _, _) =>
        tabularize(name, dataType.simpleString, "null")
    }
  }

  /** Pads all the given strings to `ColumnWidth` and adds a tab afterwards */
  private def tabularize(strings: String*): String =
    strings.map(_.padTo(ColumnWidth, " ").mkString("") + "\t").mkString("")

  private def headerize(string: String): String = s"# $string"

  override lazy val output: Seq[Attribute] =
    AttributeReference("result", StringType)() :: Nil
}

object DescribeFormattedCommand {
  private val ColumnWidth = 20
}
