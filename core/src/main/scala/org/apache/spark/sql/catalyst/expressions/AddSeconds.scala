package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.util.DateTimeUtils

/**
  * Adds a number of seconds to a timestamp.
  */
case class AddSeconds(timestamp: Expression, seconds: Expression)
  extends BinaryExpression
  with ImplicitCastInputTypes
  with CodegenFallback {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, IntegerType)

  override def nullSafeEval(microseconds: Any, seconds: Any): Any = {
    microseconds.asInstanceOf[DateTimeUtils.SQLTimestamp] +
      (seconds.asInstanceOf[Int] * DateTimeUtils.MICROS_PER_SECOND)
  }

  override def left: Expression = timestamp
  override def right: Expression = seconds
  override def dataType: DataType = TimestampType
}


