package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.describable.Describable
import org.apache.spark.sql.sources.describable.FieldLike.StructFieldLike
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Command to get debug information of a table.
 */
private[sql] case class DeepDescribeCommand(
    relation: Describable)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val description = relation.describe()
    Seq(description match {
      case r: Row => r
      case default => Row(default)
    })
  }

  override def output: Seq[Attribute] = {
    relation.describeOutput match {
      case StructType(fields) =>
        fields.map(StructFieldLike.toAttribute)
      case other =>
        AttributeReference("value", other)() :: Nil
    }
  }
}
