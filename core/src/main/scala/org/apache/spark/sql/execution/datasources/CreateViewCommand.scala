package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{NonPersistedView, LogicalPlan}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.{Row, SQLContext}
import SqlContextAccessor._

/**
  * Execution for CREATE TEMPORARY VIEW commands.
  *
  * @param name View name.
  * @param isTemporary true if the view is temporary, otherwise false.
  * @param query The underlying query for the view.
  */
private[sql]
case class CreateViewCommand(name: String, isTemporary: Boolean,
                             query: LogicalPlan) extends RunnableCommand {

  override def output: Seq[Attribute] = Nil
  override def children: Seq[LogicalPlan] = query :: Nil

  override def run(sqlContext: SQLContext): Seq[Row] = query match {
    case NonPersistedView(child) =>
      if(!isTemporary) {
        log.warn(s"The view: $name will be temporary although it is marked as non-temporary!" +
          s" in order to create a persistent view please use: 'CREATE VIEW ... USING' syntax")
      }

      if (sqlContext.tableNames().contains(name)) {
        sys.error(s"View $name already exists")
      }

      sqlContext.registerRawPlan(child, name)
      Seq.empty[Row]

    case _ =>
      throw new IllegalArgumentException(s"the ${getClass.getSimpleName} " +
      s"expects a ${NonPersistedView.getClass.getSimpleName} plan")
  }
}
