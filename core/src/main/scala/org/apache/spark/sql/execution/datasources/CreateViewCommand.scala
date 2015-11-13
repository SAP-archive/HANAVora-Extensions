package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Execution for CREATE VIEW commands.
  *
  * @param name View name.
  * @param query The underlying query for the view.
  */
private[sql]
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
