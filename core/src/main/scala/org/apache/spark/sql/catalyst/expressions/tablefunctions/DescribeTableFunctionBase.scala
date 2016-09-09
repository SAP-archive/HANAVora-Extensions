package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.TableFunction
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.tablefunctions.{LogicalPlanExtractor, OutputFormatter}

/**
  * Base implementation of describe table functionality.
  */
trait DescribeTableFunctionBase extends TableFunction {

  /** @inheritdoc */
  def execute(sqlContext: SQLContext, plan: LogicalPlan): Seq[SparkPlan] = {
    val extractor = LogicalPlanExtractor(plan)
    val data = extractor.columns.flatMap { column =>
      val nonEmptyAnnotations =
        if (column.annotations.isEmpty) {
          Map((null, null))
        } else column.annotations
      new OutputFormatter(
        extractor.tableSchema,
        column.tableName,
        column.name,
        column.originalTableName,
        column.originalName,
        column.index,
        column.isNullable,
        column.inferredSqlType,
        column.numericPrecision.orNull,
        column.numericPrecisionRadix.orNull,
        column.numericScale.orNull,
        nonEmptyAnnotations).format()
    }
    createOutputPlan(data) :: Nil
  }

  /** @inheritdoc */
  override def output: Seq[Attribute] = DescribeTableStructure.output

}
