package org.apache.spark.sql.currency.basic

import org.apache.spark.sql.currency.ConversionRateNotFoundException
import org.scalatest.FunSuite

import scala.util.{Failure, Success}

import org.apache.spark.sql.currency.TestUtils._

class BasicCurrencyConversionSuite extends FunSuite {

  val one = new java.math.BigDecimal("1.0")
  val notOne = new java.math.BigDecimal("99.0")

  val RATES = Seq(
      (("EUR", "USD"),
       Seq(("2015-01-01", 1.0), ("2015-01-05", 1.1), ("2015-01-10", 1.2))),
      (("USD", "EUR"),
       Seq(("2015-01-01", 2.0), ("2015-01-05", 2.1), ("2015-01-10", 2.2))),
      (("EUR", "DKM"),
       Seq(("2015-01-01", 1.0), ("2015-01-05", 1.1), ("2015-01-10", 1.2)))
  )
    .map {
      case (currPair, rates) =>
        val decRates = rates.map {
          case (date, rate) => (date, java.math.BigDecimal.valueOf(rate))
        }

        (currPair, decRates )
    }
  val RATES_MAP = new DualKeyPartialSortedMap[(String, String), Int, java.math.BigDecimal]
  RATES.foreach {
    case (currKey, rateTuples) =>
      rateTuples.foreach {
        case (dateString, rate) =>
          val dateKey = dateString.replaceAll("-", "").toInt
          RATES_MAP.put(currKey, dateKey, rate)
      }
  }

  test("test default behaviour") {
    val basicCurrencyConversion = new BasicCurrencyConversion(
        RATES_MAP, allowInverse = false, errorHandling = ERROR_HANDLING_FAIL)


    basicCurrencyConversion.convert(one, "EUR", "USD", "2014-12-01") match {
      case Failure(ex) if ex.isInstanceOf[ConversionRateNotFoundException] =>
        assert(true)
      case Success(converted) =>
        assert(false)
    }
    basicCurrencyConversion.convert(one, "EUR", "USD", "2015-01-01") match {
      case Success(converted) => assertComparesEqual(converted.get)("1.0")
      case _ => assert(false)
    }
    basicCurrencyConversion.convert(one, "EUR", "USD", "2015-01-05") match {
      case Success(converted) => assertComparesEqual(converted.get)("1.1")
      case _ => assert(false)
    }
    basicCurrencyConversion.convert(one, "EUR", "USD", "2015-01-10") match {
      case Success(converted) => assertComparesEqual(converted.get)("1.2")
      case _ => assert(false)
    }
    basicCurrencyConversion.convert(one, "EUR", "USD", "2015-01-15") match {
      case Success(converted) => assertComparesEqual(converted.get)("1.2")
      case _ => assert(false)
    }
  }

  test("check error handling") {
    val basicCurrencyConversionFail = new BasicCurrencyConversion(
      RATES_MAP, allowInverse = false, errorHandling = ERROR_HANDLING_FAIL)

    basicCurrencyConversionFail.convert(notOne, "FOO", "BAR", "2015-01-01") match {
      case Failure(ex) if ex.isInstanceOf[ConversionRateNotFoundException] =>
        assert(true)
      case _ => assert(false)
    }

    basicCurrencyConversionFail.convert(notOne, "EUR", "USD", "2014-01-01") match {
      case Failure(ex) if ex.isInstanceOf[ConversionRateNotFoundException] =>
        assert(true)
      case _ => assert(false)
    }

    val basicCurrencyConversionNull = new BasicCurrencyConversion(
      RATES_MAP, allowInverse = false, errorHandling = ERROR_HANDLING_NULL)

    basicCurrencyConversionNull.convert(notOne, "FOO", "BAR", "2015-01-01") match {
      case Success(converted) =>
        assert(converted.isEmpty)
      case _ => assert(false)
    }

    basicCurrencyConversionNull.convert(notOne, "EUR", "USD", "2014-01-01") match {
      case Success(converted) =>
        assert(converted.isEmpty)
      case _ => assert(false)
    }

    val basicCurrencyConversionKeep = new BasicCurrencyConversion(
      RATES_MAP, allowInverse = false, errorHandling = ERROR_HANDLING_KEEP)

    basicCurrencyConversionKeep.convert(notOne, "FOO", "BAR", "2015-01-01") match {
      case Success(converted) =>
        assert(converted.get == notOne)
      case _ => assert(false)
    }

    basicCurrencyConversionKeep.convert(notOne, "EUR", "USD", "2014-01-01") match {
      case Success(converted) =>
        assert(converted.get == notOne)
      case _ => assert(false)
    }
  }

  test("check allow inverse") {
    val basicCurrencyConversionNoInverse = new BasicCurrencyConversion(
      RATES_MAP, allowInverse = false, errorHandling = ERROR_HANDLING_FAIL)

    basicCurrencyConversionNoInverse.convert(one, "DKM", "EUR", "2015-01-01") match {
      case Failure(ex) if ex.isInstanceOf[ConversionRateNotFoundException] =>
        assert(true)
      case _ => assert(false)
    }

    val basicCurrencyConversionWithInverse = new BasicCurrencyConversion(
      RATES_MAP, allowInverse = true, errorHandling = ERROR_HANDLING_FAIL)

    basicCurrencyConversionWithInverse.convert(one, "DKM", "EUR", "2015-01-01") match {
      case Success(converted) =>
        assert(converted.get == one)
      case _ => assert(false)
    }
  }
}
