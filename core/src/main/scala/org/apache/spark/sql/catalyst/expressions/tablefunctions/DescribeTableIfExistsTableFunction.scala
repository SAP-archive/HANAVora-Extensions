package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.catalyst.analysis.{Analyzer, TableFunction}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.tablefunctions.{LogicalPlanExtractor, OutputFormatter}
import org.apache.spark.sql.extension.ExtendedPlanner

import scala.util.Try

class DescribeTableIfExistsTableFunction extends TableFunction {
  /** @inheritdoc */
  override def apply(planner: ExtendedPlanner)
                    (arguments: Seq[Any]): Seq[SparkPlan] = arguments match {
    case Seq(Some(plan: LogicalPlan)) =>
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

  /** @inheritdoc */
  override def output: Seq[Attribute] =
    DescribeTableStructure.output
}
