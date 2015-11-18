package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.sources.DescribableRelation

/**
 * Command to get debug information of a table.
 */
case class DescribeDatasourceCommand(
    relation: Option[DescribableRelation])
  extends RunnableCommand {
  override def run(sqlContext: SQLContext): Seq[Row] = relation match {
    case None => sys.error(s"Could not find a relation to describe.")

    case Some(describable) =>
      val description = describable.describe
      description.map({
        case (key, value) => Row(key, value)
      }).toSeq
  }
}
