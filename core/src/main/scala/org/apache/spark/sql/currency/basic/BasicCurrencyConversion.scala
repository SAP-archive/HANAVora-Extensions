package org.apache.spark.sql.currency.basic

import org.apache.spark.sql.currency.ConversionRateNotFoundException
import org.apache.spark.sql.currency.basic.BasicCurrencyConversion.RatesMap

import scala.util.{Failure, Success, Try}

object BasicCurrencyConversion {
  type CurrencyKey = (String, String)
  type DateKey = Int
  type RatesMap = DualKeyPartialSortedMap[CurrencyKey, DateKey, java.math.BigDecimal]
}

/**
  * A basic currency conversion implementation.
  * Conversions are done based on rate values in given [[DualKeyPartialSortedMap]].
  *
  * @param rates The rate values given as [[DualKeyPartialSortedMap]].
  * @param allowInverse If true, a conversion from A to B with missing rate information
  *                     is tried to be realized by a conversion from the B to A rate value
  *                     (if it exists).
  * @param errorHandling One of "set_to_null" (return None), "keep_unconverted" (return given
  *                      amount), or "fail_on_error" (returns a [[Failure]]). The strings
  *                      are taken from the HANA convert_currency method and used here
  *                      to be compatible.
  */
class BasicCurrencyConversion(
    rates: RatesMap,
    allowInverse: Boolean,
    errorHandling: String)
  extends Serializable {

  /**
    * Converts an amount given in currency "from" to the amount in currency "to".
    *
    * @param amount The amount in "from" currency
    * @param from The currency in which the amount is given
    * @param to The currency to which the amount should be converted
    * @param date The date on which the conversion should happen
    * @return The amount in "to" currency". The amount is returend as a [[Try[Option[Double]].
    *         The [[Try]] will fail, if the conversion could not be performend (since the
    *         rate information is missing) and the [[errorHandling]] option is set to
    *         "fail_on_error". Else, the [[Try]] will succeed, and the value will be
    *         either [[Some(Double)]] (if conversion could be applied), or [[None]], in
    *         case the conversion could not be applied, but [[errorHandling]] is set to
    *         "set_to_null". The unconverted amount will be returned if [[errorHandling]]
    *         is set to "keep_unconverted".
    */
  def convert(amount: java.math.BigDecimal, from: String, to: String, date: String):
  Try[Option[java.math.BigDecimal]] = {
    val dateKey = date.replaceAll("-", "").toInt
    val currencyKey = (from, to)
    val converted = rates.getSortedKeyFloorValue(currencyKey, dateKey) match {
      case Some(rate) => Some(amount.multiply(rate))
      case None if allowInverse =>
        val invCurrencyKey = (to, from)
        rates.getSortedKeyFloorValue(invCurrencyKey, dateKey) match {
          case Some(rate) => Some(amount.divide(rate, java.math.RoundingMode.HALF_EVEN))
          case _ => None
        }
      case _ => None
    }
    applyErrorHandling(converted, amount, from, to, date)
  }

  private[this] def applyErrorHandling(convertedAmount: Option[java.math.BigDecimal],
                                       origAmount: java.math.BigDecimal,
                                       from: String,
                                       to: String,
                                       date: String): Try[Option[java.math.BigDecimal]] = {
    convertedAmount match {
      case Some(_converted) => Success(Some(_converted))
      case None if errorHandling.equalsIgnoreCase(ERROR_HANDLING_FAIL) =>
        Failure(new ConversionRateNotFoundException(
          s"Rate for [$from] to [$to] conversion >= [$date] does not exist"))
      case None if errorHandling.equalsIgnoreCase(ERROR_HANDLING_NULL) =>
        Success(None)
      case None if errorHandling.equalsIgnoreCase(ERROR_HANDLING_KEEP) =>
        Success(Some(origAmount))
    }
  }
}

