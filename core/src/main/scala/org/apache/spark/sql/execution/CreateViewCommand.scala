package org.apache.spark.sql.execution

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.DropRelation

case class CreateViewCommand(name: String, query: LogicalPlan) extends RunnableCommand {

  override def output: Seq[Attribute] = Nil
  override def children: Seq[LogicalPlan] = query :: Nil

  override def run(sqlContext: SQLContext): Seq[Row] = {
    if (sqlContext.tableNames().contains(name)) {
      sys.error(s"Table $name already exists")
    }
    val df = DataFrame(sqlContext, query)
    sqlContext.registerDataFrameAsTable(df, name)
    Seq.empty[Row]
  }
}
