package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.catalyst.analysis.TableFunction
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.tablefunctions.RunDescribeTable
import org.apache.spark.sql.extension.ExtendedPlanner

/** A function that describes the given argument in form of a table. */
class DescribeTableFunction extends TableFunction {
  override def apply(planner: ExtendedPlanner)
                    (arguments: Seq[LogicalPlan]): Seq[SparkPlan] = arguments match {
    case Seq(plan: LogicalPlan) =>
      RunDescribeTable(plan) :: Nil

    case _ => throw new IllegalArgumentException("Wrong number of arguments given (1 required)")
  }

  override def output: Seq[Attribute] = DescribeTableStructure.output
}

