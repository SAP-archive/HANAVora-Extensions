package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources.DropRelation

case class DropRunnableCommand(relation: DropRelation) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    relation.dropTable()
    Seq.empty[Row]
  }
}
