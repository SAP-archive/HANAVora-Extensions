package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.analysis.ResolvedTableFunction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.extension.ExtendedPlanner

/** Applies resolved table functions with the planner and their arguments. */
case class TableFunctionsStrategy(planner: ExtendedPlanner) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ResolvedTableFunction(f, arguments) =>
      f(planner)(arguments)

    case _ => Nil
  }
}
