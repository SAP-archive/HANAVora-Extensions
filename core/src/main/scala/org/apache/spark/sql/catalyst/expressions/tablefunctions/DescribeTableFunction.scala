package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.catalyst.analysis.{Analyzer, TableFunction}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.tablefunctions.{LogicalPlanExtractor, OutputFormatter}
import org.apache.spark.sql.extension.ExtendedPlanner
import org.apache.spark.sql.sources.sql.SqlBuilder

/** A function that describes the given argument in form of a table. */
class DescribeTableFunction extends DescribeTableFunctionBase {
  override def apply(planner: ExtendedPlanner)
                    (arguments: Seq[Any]): Seq[SparkPlan] = arguments match {
    case Seq(plan: LogicalPlan) =>
      execute(plan)

    case _ => throw new IllegalArgumentException("Wrong number of arguments given (1 required)")
  }
}
