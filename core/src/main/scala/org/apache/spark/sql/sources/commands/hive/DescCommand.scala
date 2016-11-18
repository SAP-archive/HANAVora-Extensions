package org.apache.spark.sql.sources.commands.hive

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SQLContext}

/**
  * An emulated hive command to describe a relation.
  *
  * @param ident The name of the relation to describe.
  */
case class DescCommand(ident: TableIdentifier) extends HiveRunnableCommand {

  override protected val commandName: String = s"DESC $ident"

  override def execute(sqlContext: SQLContext): Seq[Row] = {
    val plan = sqlContext.catalog.lookupRelation(ident)
    if (plan.resolved) {
      plan.schema.map { field =>
        Row(field.name, field.dataType.simpleString, None)
      }
    } else {
      Seq.empty
    }
  }

  override lazy val output: Seq[Attribute] =
    AttributeReference("col_name", StringType)() ::
    AttributeReference("data_type", StringType)() ::
    AttributeReference("comment", StringType)() :: Nil
}
