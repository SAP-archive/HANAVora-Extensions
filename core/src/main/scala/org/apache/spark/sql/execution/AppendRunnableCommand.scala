package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.AppendRelation

case class AppendRunnableCommand(
    relation: AppendRelation,
    options: Map[String, String])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    relation.appendFilesToTable(options)
    Seq.empty[Row]
  }
}
