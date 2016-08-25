package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.catalyst.analysis.{Analyzer, TableFunction}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.tablefunctions.{LogicalPlanExtractor, OutputFormatter}
import org.apache.spark.sql.extension.ExtendedPlanner

import scala.util.Try

class DescribeTableIfExistsTableFunction extends DescribeTableFunctionBase {
  /** @inheritdoc */
  override def apply(planner: ExtendedPlanner)
                    (arguments: Seq[Any]): Seq[SparkPlan] = arguments match {
    case Seq(Some(plan: LogicalPlan)) =>
      execute(planner.sqlContext, plan)

    case Seq(None) =>
      createOutputPlan(Nil) :: Nil

    case default =>
      throw new IllegalArgumentException("Invalid number of arguments provided " +
        s"1 expected, ${default.size} given")
  }


  /** @inheritdoc */
  override def analyze(analyzer: Analyzer, arguments: Seq[LogicalPlan]): Seq[Any] = {
    arguments.map(plan => Try(analyzer.execute(plan)).toOption)
  }
}
