package org.apache.spark.sql.currency

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.currency.basic.BasicCurrencyConversionFunction
import org.apache.spark.sql.currency.erp.ERPCurrencyConversionFunction

/**
  * This is the primary interface to register currency conversion functions to SparkSQL.
  */
object CurrencyConversionFunction {
  // maps SQL function names to implementations
  val functions = Map("simple_cc" -> BasicCurrencyConversionFunction,
                      "convert_currency" -> ERPCurrencyConversionFunction)
}

/**
  * Any implementation of a currency function.
  * It must implement the [getExpression] function, that allows to register the
  * implementation as a SQL expression.
  *
  * All [[CurrencyConversionFunction]] implementations must be registered in the
  * [[CurrencyConversionFunction.functions]] sequence.
  */
trait CurrencyConversionFunction {

  /**
    * Creates the expression used to perform this conversion. This expression will be
    * serialized and be shipped to the workers. Implementations may update their internal state
    * on the driver and use serializable/stateful references inside the [[Expression]] instance.
    * The [[Expression]] gets retrieved in SparkSQL's analysis phase.
    *
    * @param children child expressions
    * @return
    */
  def getExpression(children: Seq[Expression]): Expression
}
