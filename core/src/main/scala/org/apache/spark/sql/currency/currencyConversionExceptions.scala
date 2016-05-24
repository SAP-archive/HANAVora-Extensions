package org.apache.spark.sql.currency

class CurrencyConversionException(msg: String, cause: Throwable = null)
  extends RuntimeException(msg, cause)

class ConversionRateNotFoundException(msg: String, cause: Throwable = null)
  extends CurrencyConversionException(msg, cause)

