package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, NonPersistedCubeView, NonPersistedDimensionView}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.execution.datasources.SqlContextAccessor._
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Execution for CREATE TEMPORARY CUBE VIEW commands.
 *
 * @param name Cube view name.
 * @param isTemporary true if the view is temporary, otherwise false.
 * @param query The underlying query for the view.
 */
private[sql]
case class CreateCubeViewCommand(name: String, isTemporary: Boolean,
                             query: LogicalPlan) extends RunnableCommand {

  override def output: Seq[Attribute] = Nil
  override def children: Seq[LogicalPlan] = query :: Nil

  override def run(sqlContext: SQLContext): Seq[Row] = query match {
    case NonPersistedCubeView(child) =>
      if(!isTemporary) {
        log.warn(s"The cube view: $name will be temporary although it is " +
          s"marked as non-temporary! in order to create a persistent view please use: " +
          s"'CREATE CUBE VIEW ... USING' syntax")
      }

      if (sqlContext.catalog.tableExists(Seq(name))) {
        sys.error(s"$name already exists")
      }

      sqlContext.registerRawPlan(child, name)

      Seq.empty[Row]

    case _ =>
      throw new IllegalArgumentException(s"the ${getClass.getSimpleName} " +
        s"expects a ${NonPersistedCubeView.getClass.getSimpleName} plan")
  }
}
