package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.catalyst.analysis.{Analyzer, TableFunction}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.tablefunctions.{LogicalPlanExtractor, OutputFormatter}
import org.apache.spark.sql.extension.ExtendedPlanner

/** A function that describes the given argument in form of a table. */
class DescribeTableFunction extends TableFunction {
  override def apply(planner: ExtendedPlanner)
                    (arguments: Seq[Any]): Seq[SparkPlan] = arguments match {
    case Seq(plan: LogicalPlan) =>
      val extractor = new LogicalPlanExtractor(plan)
      val data = extractor.columns.flatMap { column =>
        new OutputFormatter(
          extractor.tableSchema,
          column.tableName,
          column.name,
          column.index,
          column.isNullable,
          column.dataType,
          column.numericPrecision,
          column.numericPrecisionRadix,
          column.numericScale,
          column.nonEmptyAnnotations).format()
      }
      createOutputPlan(data) :: Nil

    case _ => throw new IllegalArgumentException("Wrong number of arguments given (1 required)")
  }

  override def output: Seq[Attribute] = DescribeTableStructure.output
}

