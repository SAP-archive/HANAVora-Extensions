package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.DescribableRelation
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Command to get debug information of a table.
 */
private[sql] case class DescribeDatasourceCommand(
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
