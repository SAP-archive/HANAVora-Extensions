package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{NonPersistedDimensionView, NonPersistedView, View, LogicalPlan}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import SqlContextAccessor._

/**
 * Execution for CREATE TEMPORARY DIMENSION VIEW commands.
 *
 * @param name Dimension view name.
 * @param isTemporary true if the view is temporary, otherwise false.
 * @param query The underlying query for the view.
 */
private[sql]
case class CreateDimensionViewCommand(name: String, isTemporary: Boolean,
                             query: LogicalPlan) extends RunnableCommand {

  override def output: Seq[Attribute] = Nil
  override def children: Seq[LogicalPlan] = query :: Nil

  override def run(sqlContext: SQLContext): Seq[Row] = query match {
    case NonPersistedDimensionView(child) =>
      if(!isTemporary) {
        log.warn(s"The dimension view: $name will be temporary although it is " +
          s"marked as non-temporary! in order to create a persistent view please use: " +
          s"'CREATE DIMENSION VIEW ... USING' syntax")
      }

      if (sqlContext.tableNames().contains(name)) {
        sys.error(s"$name already exists")
      }

      sqlContext.registerRawPlan(child, name)

      Seq.empty[Row]

    case _ =>
      throw new IllegalArgumentException(s"the ${getClass.getSimpleName} " +
        s"expects a ${NonPersistedDimensionView.getClass.getSimpleName} plan")
  }
}
