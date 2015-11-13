package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.compat._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.compat._

/** Return ed plus en years as new date */
case class AddYears(date: Expression, years: Expression)
  extends BackportedBinaryExpression
  with ImplicitCastInputTypes with CodegenFallback {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def nullSafeEval(d: Any, y: Any): Any = {
    DateTimeUtils.dateAddMonths(
      d.asInstanceOf[DateTimeUtils.SQLDate], y.asInstanceOf[Int] * 12
    )
  }

  override def left: Expression = date
  override def right: Expression = years
  override def dataType: DataType = DateType
}
