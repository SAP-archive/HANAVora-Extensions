package org.apache.spark.sql.currency.basic

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Implements a fallback currency conversion based on a simple rates table.
 *
 * @param conversion The basic conversion instance
 * @param children expressions (amount, source currency, target currency, date)
 */
case class BasicCurrencyConversionExpression(
    conversion: BasicCurrencyConversion,
    children: Seq[Expression])
  extends Expression
  with ImplicitCastInputTypes
  with CodegenFallback {

  protected val AMOUNT_INDEX = 0
  protected val FROM_INDEX = 1
  protected val TO_INDEX = 2
  protected val DATE_INDEX = 3
  protected val NUM_ARGS = 4

  override def eval(input: InternalRow): Any = {
    val inputArguments = children.map(_.eval(input))

    require(inputArguments.length == NUM_ARGS, "wrong number of arguments")

    val sourceCurrency =
      Option(inputArguments(FROM_INDEX).asInstanceOf[UTF8String]).map(_.toString)
    val targetCurrency = Option(inputArguments(TO_INDEX).asInstanceOf[UTF8String]).map(_.toString)
    val amount = Option(inputArguments(AMOUNT_INDEX).asInstanceOf[Double])
    val date = Option(inputArguments(DATE_INDEX).asInstanceOf[UTF8String]).map(_.toString)

    (amount, sourceCurrency, targetCurrency, date) match {
      case (Some(a), Some(s), Some(t), Some(d)) => nullSafeEval(a, s, t, d)
      case _ => null
    }
  }

  def nullSafeEval(amount: Double,
                   sourceCurrency: String,
                   targetCurrency: String,
                   date: String): Any = {
    conversion.convert(amount, sourceCurrency, targetCurrency, date).get.orNull
  }

  override def dataType: DataType = DoubleType

  override def nullable: Boolean = true

  // TODO(MD, CS): use DateType but support date string
  override def inputTypes: Seq[AbstractDataType] =
    Seq(DoubleType, StringType, StringType, StringType)
}

