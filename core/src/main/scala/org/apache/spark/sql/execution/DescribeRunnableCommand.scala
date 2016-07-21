package org.apache.spark.sql.execution

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.tablefunctions.{LogicalPlanExtractor, OutputFormatter}
import org.apache.spark.sql.types._

/**
 * Returns the description of a select query.
 *
 * The query must be a SELECT statement. Moreover, it is allowed for the SELECT statement
 * to have extra annotations for retrieving specific meta data.
 */
// TODO (AC) Remove this once table-valued function are rebased on top.
private[sql]
case class DescribeRunnableCommand(plan: LogicalPlan) extends RunnableCommand {

  override val output = StructType(
      StructField("NAME", StringType, nullable = false) ::
      StructField("POSITION", IntegerType, nullable = false) ::
      StructField("DATA_TYPE", StringType, nullable = false) ::
      StructField("ANNOTATION_KEY", StringType, nullable = true) ::
      StructField("ANNOTATION_VALUE", StringType, nullable = true) ::
      Nil).toAttributes

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val extractor = new LogicalPlanExtractor(plan)
    extractor.columns.flatMap { column =>
      val nonEmptyAnnotations =
        if (column.annotations.isEmpty) {
          Map((null, null))
        } else {
          column.annotations
        }
      new OutputFormatter(
        column.name,
        column.index - 1, // OLAP_DESCRIBE expects indices to start with 0
        column.inferredSqlType,
        nonEmptyAnnotations).format().map(Row.fromSeq)
    }
  }
}
