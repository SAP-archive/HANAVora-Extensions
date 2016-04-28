package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.tablefunctions.SimpleTableFunctionOutput
import org.apache.spark.sql.extension.ExtendedPlanner

/**
 * A function that has access to analysis and planning phase and returns [[SparkPlan]]s.
 */
trait TableFunction {
  /** Analyzes the arguments with the given analyzer.
    *
    * By default, this executes the analyzer on each argument.
    * @param analyzer The analyzer
    * @param arguments The arguments of the table function.
    * @return The analyzed arguments.
    */
  def analyze(analyzer: Analyzer, arguments: Seq[LogicalPlan]): Seq[Any] = {
    arguments.map(analyzer.execute)
  }

  /** Plans and returns a physical representation of the table function with the given arguments.
    *
    * @param planner The planner
    * @param arguments The arguments of the table function.
    * @return A sequence of generated [[SparkPlan]]s.
    */
  def apply(planner: ExtendedPlanner)(arguments: Seq[Any]): Seq[SparkPlan]

  /**
    * Creates a [[SimpleTableFunctionOutput]] physical plan from the given data.
    *
    * @param data The data of the output.
    * @return A [[SimpleTableFunctionOutput]] spark physical plan.
    */
  def createOutputPlan(data: Seq[Seq[Any]]): SimpleTableFunctionOutput =
    SimpleTableFunctionOutput(data, output)

  /** The output structure of the physical plan.
    *
    * @return The output structure of the physical plan.
    */
  def output: Seq[Attribute]
}
