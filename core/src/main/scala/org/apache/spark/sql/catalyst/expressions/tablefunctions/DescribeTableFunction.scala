package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.catalyst.analysis.TableFunction
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.extension.ExtendedPlanner
import org.apache.spark.sql.types.{IntegerType, StringType}

/** A function that describes the given argument in form of a table. */
class DescribeTableFunction extends TableFunction {
  override def apply(planner: ExtendedPlanner)
                    (arguments: Seq[LogicalPlan]): Seq[SparkPlan] = arguments match {
    case Seq(plan: LogicalPlan) =>
      RunDescribeTable(plan) :: Nil

    case _ => throw new IllegalArgumentException("Wrong number of arguments given (1 required)")
  }

  override def output: Seq[Attribute] = DescribeTableFunction.output
}

object DescribeTableFunction {
  /** The structure of the describe table function call result. */
  val structure =
    "TABLE_CATALOG" -> StringType ::
    "TABLE_SCHEMA" -> StringType ::
    "TABLE_NAME" -> StringType ::
    "COLUMN_NAME" -> StringType ::
    "ORDINAL_POSITION" -> IntegerType ::
    "COLUMN_DEFAULT" -> StringType ::
    "IS_NULLABLE" -> StringType ::
    "DATA_TYPE" -> StringType ::
    "CHARACTER_MAXIMUM_LENGTH" -> IntegerType ::
    "CHARACTER_OCTET_LENGTH" -> IntegerType ::
    "NUMERIC_PRECISION" -> IntegerType ::
    "NUMERIC_PRECISION_RADIX" -> IntegerType ::
    "NUMERIC_SCALE" -> IntegerType ::
    "DATETIME_PRECISION" -> IntegerType ::
    "CHARACTER_SET_CATALOG" -> StringType ::
    "CHARACTER_SET_SCHEMA" -> StringType ::
    "CHARACTER_SET_NAME" -> StringType ::
    "COLLATION_CATALOG" -> StringType ::
    "COLLATION_SCHEMA" -> StringType ::
    "COLLATION_NAME" -> StringType :: Nil

  lazy val output: Seq[Attribute] =
    structure.map { case (name, dataType) => AttributeReference(name, dataType)() }
}
