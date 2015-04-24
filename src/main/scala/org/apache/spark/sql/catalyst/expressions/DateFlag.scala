package org.apache.spark.sql.catalyst.expressions

object DateFlag extends Enumeration {
  type DateFlag = Value

  val DAY = Value("DAY")
  val MONTH = Value("MONTH")
  val YEAR = Value("YEAR")
  val HOUR = Value("HOUR")
  val MINUTE = Value("MINUTE")
  val SECOND = Value("SECOND")
}
